#![allow(internal_features)]
#![feature(thread_local)]
#![feature(core_intrinsics)]
#![feature(variant_count)]

#[macro_use]
mod utils;

pub mod mem;
pub mod net;
pub mod profile;
pub mod thread;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
