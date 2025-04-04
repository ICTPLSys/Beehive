mod allocator_utils;
mod entry;
pub(crate) mod evacuator;
pub(crate) mod local_allocator;
pub mod manager;
mod object;
mod pointer;
pub(crate) mod remote_allocator;
mod scope;

pub use pointer::{RemPtr, RemRef, RemRefMut, RemRefTrait};
pub use remote_allocator::RemoteAddr;
pub use scope::{
    DerefScopeBaseTrait, DerefScopeTrait, RootScope, SingleDerefScope, pin_remref, unpin_remref,
};
