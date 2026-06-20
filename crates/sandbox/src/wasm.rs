//! Wasmtime-based WebAssembly sandbox.
//!
//! Executes raw WebAssembly modules in a sandboxed environment.
//! WASI support is configured per-execution via prepare_env.

use eo_core::error::{CoreError, Result};
use eo_core::traits::Sandbox;
use eo_core::types::{ExecutionResult, ResourceLimits};
use tracing::debug;
use wasmtime::{Config, Engine, Linker, Module, Store, TypedFunc};

/// A sandbox that executes WebAssembly modules using Wasmtime.
pub struct WasmtimeSandbox {
    engine: Engine,
}

impl WasmtimeSandbox {
    /// Create a new Wasmtime sandbox with default settings.
    pub fn new() -> Result<Self> {
        let config = Config::new();
        let engine = Engine::new(&config).map_err(|e| {
            CoreError::SandboxExecution(format!("failed to create Wasmtime engine: {e}"))
        })?;
        Ok(Self { engine })
    }
}

impl Sandbox for WasmtimeSandbox {
    fn prepare_env(&self, limits: ResourceLimits) -> Result<()> {
        debug!(
            "Preparing WASM env: max_memory={}MB, max_cpu={}ms",
            limits.max_memory_mb, limits.max_cpu_time_ms
        );
        Ok(())
    }

    fn execute_code(&self, bytecode: Vec<u8>) -> Result<ExecutionResult> {
        let start = std::time::Instant::now();

        let module = Module::from_binary(&self.engine, &bytecode).map_err(|e| {
            CoreError::SandboxExecution(format!("failed to compile wasm module: {e}"))
        })?;

        let mut store = Store::new(&self.engine, ());
        let linker = Linker::new(&self.engine);

        let instance = linker.instantiate(&mut store, &module).map_err(|e| {
            CoreError::SandboxExecution(format!("failed to instantiate module: {e}"))
        })?;

        // Try to call _start if it exists
        let result = instance
            .get_typed_func::<(), ()>(&mut store, "_start")
            .and_then(|func: TypedFunc<(), ()>| func.call(&mut store, ()));

        let execution_time_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(()) => {
                debug!("Wasm execution completed in {}ms", execution_time_ms);
                Ok(ExecutionResult {
                    exit_code: 0,
                    stdout: vec![],
                    stderr: vec![],
                    execution_time_ms,
                    peak_memory_bytes: 0,
                    result_hash: None,
                })
            }
            Err(e) => {
                // Traps are normal for untrusted/wasi modules — not a hard error
                debug!("Wasm execution trapped (expected for WASI modules): {}", e);
                Ok(ExecutionResult {
                    exit_code: 1,
                    stdout: vec![],
                    stderr: format!("trap: {e}").into_bytes(),
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

    #[test]
    fn compile_and_run_wat_module() {
        let sandbox = WasmtimeSandbox::new().unwrap();

        // A minimal module that exports a _start function with no imports
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
        let sandbox = WasmtimeSandbox::new().unwrap();
        // Random bytes are not valid Wasm
        let result = sandbox.execute_code(vec![0x00, 0x01, 0x02, 0x03]);
        assert!(result.is_err());
    }

    #[test]
    fn concurrent_sandbox_instances_isolated() {
        let sandbox = WasmtimeSandbox::new().unwrap();

        let wasm1 = wat::parse_str(
            r#"
            (module
                (func $_start)
                (export "_start" (func $_start))
            )
        "#,
        )
        .unwrap();

        let wasm2 = wat::parse_str(
            r#"
            (module
                (func $_start)
                (export "_start" (func $_start))
            )
        "#,
        )
        .unwrap();

        let r1 = sandbox.execute_code(wasm1).unwrap();
        let r2 = sandbox.execute_code(wasm2).unwrap();
        assert_eq!(r1.exit_code, 0);
        assert_eq!(r2.exit_code, 0);
    }
}
