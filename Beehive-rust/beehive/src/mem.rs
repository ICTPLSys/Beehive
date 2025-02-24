mod allocator_utils;
mod entry;
mod local_allocator;
pub mod manager;
mod object;
mod pointer;
mod remote_allocator;
mod scope;

pub use pointer::{RemPtr, RemRef, RemRefMut};
pub use scope::DerefScope;
