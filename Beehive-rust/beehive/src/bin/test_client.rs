use beehive::mem::RemoteAddr;
use beehive::net::Client;

#[beehive_helper::beehive_main]
fn main() {
    unsafe {
        let memory = Client::get_memory();
        let slice = std::slice::from_raw_parts_mut(memory as *mut i64, 16);
        let length = (size_of_val(slice)) as u32;
        for (i, v) in slice.iter_mut().enumerate() {
            *v = i as i64;
        }
        Client::post_write(RemoteAddr::new(0, 0), memory as u64, length, 0x66);
        for (i, &v) in slice.iter().enumerate() {
            assert_eq!(v, i as i64);
        }
        loop {
            let n = Client::poll(|_| panic!(), |wr_id| assert_eq!(wr_id, 0x66));
            if n == 1 {
                break;
            }
            assert_eq!(n, 0);
        }
        for v in slice.iter_mut() {
            *v = 0 as i64;
        }
        Client::post_read(RemoteAddr::new(0, 0), memory as u64, length, 0x88);
        loop {
            let n = Client::poll(|wr_id| assert_eq!(wr_id, 0x88), |_| panic!());
            if n == 1 {
                break;
            }
            assert_eq!(n, 0);
        }
        for (i, &v) in slice.iter().enumerate() {
            assert_eq!(v, i as i64);
        }
    }
}
