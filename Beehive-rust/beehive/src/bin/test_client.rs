use beehive::net::Client;
use beehive::net::Config;

fn main() {
    pretty_env_logger::init();
    let config = Config::default();
    Client::initialize(config);

    unsafe {
        let memory = Client::get_memory();
        let remote_base = Client::get_remote_base_addr(0);
        let slice = std::slice::from_raw_parts_mut(memory as *mut i64, 16);
        let length = (size_of_val(slice)) as u32;
        for (i, v) in slice.iter_mut().enumerate() {
            *v = i as i64;
        }
        Client::post_write(remote_base, memory as u64, length, 0x66);
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
        Client::post_read(remote_base, memory as u64, length, 0x88);
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

    Client::destroy();
}
