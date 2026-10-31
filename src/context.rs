//! Contexts: the unit the hub would partition its registry by. The owner deferred contexts
//! indefinitely (no tenant use case); P0 keeps the door open.
//!
//! Every session lives in the `default` context. The registry, session and router APIs
//! already take a [`ContextId`] so adding contexts later changes their implementation, not their
//! callers.

use std::fmt;
use std::sync::Arc;

/// Name of the one context that exists; `Register` has no context field.
pub const DEFAULT_CONTEXT: &str = "default";

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContextId(Arc<str>);

impl ContextId {
    pub fn new(name: &str) -> Self {
        Self(Arc::from(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_default(&self) -> bool {
        &*self.0 == DEFAULT_CONTEXT
    }
}

impl Default for ContextId {
    fn default() -> Self {
        Self::new(DEFAULT_CONTEXT)
    }
}

impl fmt::Display for ContextId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_context_is_named_default() {
        let ctx = ContextId::default();
        assert_eq!(ctx.as_str(), "default");
        assert!(ctx.is_default());
        assert!(!ContextId::new("tenant-a").is_default());
        assert_eq!(ctx.to_string(), "default");
    }
}
