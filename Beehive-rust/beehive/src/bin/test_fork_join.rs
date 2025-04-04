use beehive::thread::*;
use libfibre_port::*;
fn main() {
    cfibre_init(4);
    fork_join_with_id(16, |id| {
        println!("id: {}", id);
    });
    println!("end");
}
