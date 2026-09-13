//! Wasmtime-based WebAssembly sandbox with real, verifiable resource limits.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use eo_core::error::{CoreError, Result};
use eo_core::traits::Sandbox;
use eo_core::types::{ExecutionResult, ResourceLimits};
use tracing::debug;
use wasmtime::{Config, Engine, Linker, Module, Store, StoreLimitsBuilder};

/// A completed (or failed) wasm invocation, classified for result mapping.
enum Epilog {
    Ok,
    Timeout,
    Trap(String),
}

const MAX_INSTANCES: usize = 100;
const MAX_TABLES: usize = 100;
const MAX_MEMORIES: usize = 100;

struct SandboxState {
    limits: wasmtime::StoreLimits,
    timed_out: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct WasmtimeSandbox {
    engine: Engine,
    limits: ResourceLimits,
}

impl WasmtimeSandbox {
    pub fn new() -> Result<Self> {
        Self::with_limits(ResourceLimits::default())
    }

    pub fn with_limits(limits: ResourceLimits) -> Result<Self> {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config).map_err(|e| {
            CoreError::SandboxExecution(format!("failed to create Wasmtime engine: {e}"))
        })?;
        Ok(Self { engine, limits })
    }

    fn make_store(&self) -> Store<SandboxState> {
        let mem_bytes = self.limits.max_memory_mb.saturating_mul(1024 * 1024) as usize;
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(mem_bytes)
            .instances(MAX_INSTANCES)
            .tables(MAX_TABLES)
            .memories(MAX_MEMORIES)
            .trap_on_grow_failure(true)
            .build();

        let state = SandboxState {
            limits: store_limits,
            timed_out: Arc::new(AtomicBool::new(false)),
        };
        let mut store = Store::new(&self.engine, state);
        store.limiter(|s| &mut s.limits);
        store.epoch_deadline_trap();
        store
    }

    fn run_with_epoch(
        &self,
        store: &mut Store<SandboxState>,
        func: wasmtime::TypedFunc<(), ()>,
    ) -> Epilog {
        let deadline_ms = self.limits.max_cpu_time_ms;
        store.set_epoch_deadline(deadline_ms);
        let engine = store.engine().clone();
        let timed_out = store.data().timed_out.clone();
        let stop = Arc::new(AtomicBool::new(false));

        let ticker = {
            let e = engine.clone();
            let stop_flag = stop.clone();
            std::thread::spawn(move || {
                while !stop_flag.load(Ordering::Relaxed) {
                    e.increment_epoch();
                    thread::sleep(Duration::from_millis(1));
                }
            })
        };

        let watchdog = {
            let flag = timed_out.clone();
            let stop_flag = stop.clone();
            std::thread::spawn(move || {
                let early = deadline_ms.saturating_sub(50);
                let mut slept = 0u64;
                while slept < early && !stop_flag.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(1));
                    slept += 1;
                }
                if slept >= early {
                    flag.store(true, Ordering::SeqCst);
                }
            })
        };

        let call_result = func.call(store, ());

        // Stop background threads and read the timeout flag BEFORE joining the
        // watchdog, so a fast trap is never misclassified as a timeout.
        stop.store(true, Ordering::SeqCst);
        let was_timeout = timed_out.load(Ordering::SeqCst);
        let _ = ticker.join();
        let _ = watchdog.join();

        match call_result {
            Ok(()) => Epilog::Ok,
            Err(_e) if was_timeout => Epilog::Timeout,
            Err(e) => Epilog::Trap(format!("{e:?}")),
        }
    }
}

impl Sandbox for WasmtimeSandbox {
    fn prepare_env(&self, limits: ResourceLimits) -> Result<()> {
        if limits.max_memory_mb == 0 || limits.max_cpu_time_ms == 0 {
            return Err(CoreError::SandboxExecution(
                "resource limits must be non-zero".into(),
            ));
        }
        debug!(
            "Wasm sandbox limits: {}MB mem, {}ms cpu",
            limits.max_memory_mb, limits.max_cpu_time_ms
        );
        Ok(())
    }

    fn execute_code(&self, bytecode: Vec<u8>) -> Result<ExecutionResult> {
        let start = Instant::now();

        let module = Module::from_binary(&self.engine, &bytecode).map_err(|e| {
            CoreError::SandboxExecution(format!("failed to compile wasm module: {e}"))
        })?;

        let mut store = self.make_store();
        let linker = Linker::new(&self.engine);
        let instance = linker.instantiate(&mut store, &module).map_err(|e| {
            CoreError::SandboxExecution(format!("failed to instantiate module: {e}"))
        })?;

        let epilog = match instance.get_typed_func::<(), ()>(&mut store, "_start") {
            Ok(func) => self.run_with_epoch(&mut store, func),
            Err(_) => Epilog::Ok,
        };

        let execution_time_ms = start.elapsed().as_millis() as u64;

        match epilog {
            Epilog::Ok => Ok(ExecutionResult {
                exit_code: 0,
                stdout: Vec::new(),
                stderr: Vec::new(),
                execution_time_ms,
                peak_memory_bytes: 0,
                result_hash: None,
            }),
            Epilog::Timeout => {
                debug!("Wasm interrupt after {execution_time_ms}ms");
                Ok(ExecutionResult {
                    exit_code: 124,
                    stdout: Vec::new(),
                    stderr: format!(
                        "timeout: exceeded {}ms cpu budget",
                        self.limits.max_cpu_time_ms
                    )
                    .into_bytes(),
                    execution_time_ms,
                    peak_memory_bytes: 0,
                    result_hash: None,
                })
            }
            Epilog::Trap(msg) => {
                debug!("Wasm trap: {msg}");
                Ok(ExecutionResult {
                    exit_code: 1,
                    stdout: Vec::new(),
                    stderr: format!("trap: {msg}").into_bytes(),
                    execution_time_ms,
                    peak_memory_bytes: 0,
                    result_hash: None,
                })
            }
        }
    }

    fn destroy(&self) -> Result<()> {
        debug!("Wasmtime sandbox destroyed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_sandbox() -> WasmtimeSandbox {
        WasmtimeSandbox::with_limits(ResourceLimits {
            max_memory_mb: 16,
            max_cpu_time_ms: 300,
            max_disk_mb: 100,
            allow_network: false,
            max_fds: 64,
        })
        .unwrap()
    }

    #[test]
    fn compile_and_run_wat_module() {
        let sandbox = test_sandbox();
        let wasm_bytes = wat::parse_str(
            r#"
            (module
                (func $_start)
                (export "_start" (func $_start))
            )
        "#,
        )
        .unwrap();
        let result = sandbox.execute_code(wasm_bytes).unwrap();
        assert_eq!(result.exit_code, 0);
    }

    #[test]
    fn sandbox_blocks_invalid_bytecode() {
        let sandbox = test_sandbox();
        let result = sandbox.execute_code(vec![0x00, 0x01, 0x02, 0x03]);
        assert!(result.is_err());
    }

    #[test]
    fn infinite_loop_is_interrupted_by_epoch() {
        let sandbox = test_sandbox();
        let wasm_bytes = wat::parse_str(
            r#"
            (module
                (func $_start
                    (loop $inf
                        br $inf
                    )
                )
                (export "_start" (func $_start))
            )
        "#,
        )
        .unwrap();
        let result = sandbox.execute_code(wasm_bytes).unwrap();
        assert_eq!(result.exit_code, 124);
        assert_contains(&result.stderr, "timeout");
    }

    #[test]
    fn initial_memory_over_limit_traps() {
        let sandbox = test_sandbox();
        let wasm_bytes = wat::parse_str(
            r#"
            (module
                (memory (export "mem") 4096)
                (func $_start)
                (export "_start" (func $_start))
            )
        "#,
        )
        .unwrap();
        let result = sandbox.execute_code(wasm_bytes);
        assert!(result.is_err(), "instantiation must hard-fail");
    }

    fn assert_contains(haystack: &[u8], needle: &str) {
        let text = String::from_utf8_lossy(haystack);
        assert!(
            text.contains(needle),
            "expected to contain {:?}, got {:?}",
            needle,
            text
        );
    }
}
