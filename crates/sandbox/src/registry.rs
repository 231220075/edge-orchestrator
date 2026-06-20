//! Sandbox registry — factory pattern for creating sandboxes by runtime kind.

use std::collections::HashMap;

use eo_core::error::{CoreError, Result};
use eo_core::traits::Sandbox;
use eo_core::types::{ResourceLimits, RuntimeKind};

use crate::container::LinuxContainerSandbox;
use crate::wasm::WasmtimeSandbox;

/// Factory trait for creating sandbox instances.
pub trait SandboxFactory: Send + Sync {
    /// Create a new sandbox instance.
    fn create(&self, limits: ResourceLimits) -> Result<Box<dyn Sandbox>>;
}

/// A registry mapping [`RuntimeKind`] to sandbox factories.
pub struct SandboxRegistry {
    factories: HashMap<RuntimeKind, Box<dyn SandboxFactory>>,
}

impl SandboxRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// Register a sandbox factory for a runtime kind.
    pub fn register(&mut self, kind: RuntimeKind, factory: Box<dyn SandboxFactory>) {
        self.factories.insert(kind, factory);
    }

    /// Create a sandbox instance for the given runtime kind.
    pub fn create(&self, kind: RuntimeKind, limits: ResourceLimits) -> Result<Box<dyn Sandbox>> {
        let factory = self
            .factories
            .get(&kind)
            .ok_or_else(|| CoreError::UnsupportedRuntime(format!("no factory for {kind:?}")))?;

        factory.create(limits)
    }
}

impl Default for SandboxRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Create the default sandbox registry based on the current platform.
///
/// - All platforms: Wasm
/// - Linux only: NativePosix (container)
pub fn default_registry() -> SandboxRegistry {
    let mut reg = SandboxRegistry::new();

    // Wasm is available everywhere
    reg.register(RuntimeKind::Wasm, Box::new(WasmSandboxFactory));

    // Linux container sandbox is only available on Linux
    if cfg!(target_os = "linux") {
        reg.register(RuntimeKind::NativePosix, Box::new(ContainerSandboxFactory));
        reg.register(RuntimeKind::Container, Box::new(ContainerSandboxFactory));
    }

    reg
}

// ---------------------------------------------------------------------------
// Factory implementations
// ---------------------------------------------------------------------------

struct WasmSandboxFactory;

impl SandboxFactory for WasmSandboxFactory {
    fn create(&self, _limits: ResourceLimits) -> Result<Box<dyn Sandbox>> {
        Ok(Box::new(WasmtimeSandbox::new()?))
    }
}

struct ContainerSandboxFactory;

impl SandboxFactory for ContainerSandboxFactory {
    fn create(&self, limits: ResourceLimits) -> Result<Box<dyn Sandbox>> {
        let sandbox = LinuxContainerSandbox::new()?;
        sandbox.prepare_env(limits)?;
        Ok(Box::new(sandbox))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_creates_wasm_sandbox() {
        let reg = default_registry();
        let sandbox = reg
            .create(RuntimeKind::Wasm, ResourceLimits::default())
            .unwrap();
        // Just verifying creation succeeds
        sandbox.destroy().unwrap();
    }

    #[test]
    fn registry_rejects_unknown_kind() {
        let reg = SandboxRegistry::new();
        let result = reg.create(RuntimeKind::Container, ResourceLimits::default());
        assert!(result.is_err());
    }
}
