// TODO: config with configure file
use serde::Deserialize;
use std::fs::File;
use std::io::Read;

#[derive(Deserialize, Debug)]
pub struct ServerConfig {
    pub addr: String,
    pub memory_size: usize,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub cq_entries: i32,
    pub qp_send_cap: u32,
    pub qp_recv_cap: u32,
    pub qp_max_send_sge: u32,
    pub qp_max_recv_sge: u32,
    pub qp_max_rd_atomic: u8,
    pub qp_mtu: u32,
    pub qp_min_rnr_timer: u8,
    pub qp_timeout: u8,
    pub qp_retry_cnt: u8,
    pub qp_rnr_retry: u8,
    pub ib_port: u8,
    pub client_memory_size: usize,
    pub evict_batch_size: usize,
    pub poll_cq_batch_size: usize,
    pub num_cores: usize,
    pub num_evacuate_threads: usize,
    pub servers: Vec<ServerConfig>,
}

impl Config {
    pub fn default() -> Config {
        Config {
            cq_entries: 16384,
            qp_send_cap: 128,
            qp_recv_cap: 8,
            qp_max_send_sge: 1,
            qp_max_recv_sge: 1,
            qp_max_rd_atomic: 16,
            qp_mtu: 5,            // IBV_MTU_4096
            qp_min_rnr_timer: 12, // 0.64 milliseconds delay
            qp_timeout: 8,        // 1048.576 usec (0.00104 sec)
            qp_retry_cnt: 7,
            qp_rnr_retry: 7,
            ib_port: 1,
            client_memory_size: 16 * 1024 * 1024,
            evict_batch_size: 256 * 1024,
            poll_cq_batch_size: 16,
            num_cores: 1,
            num_evacuate_threads: 1,
            servers: vec![ServerConfig {
                addr: "127.0.0.1:8888".to_string(),
                memory_size: 4 * 1024 * 1024 * 1024,
            }],
        }
    }
    pub fn load_config(path: &str) -> Config {
        let mut file = File::open(path).unwrap();
        let mut contents: String = String::new();
        let _ = file.read_to_string(&mut contents);
        // convert to BeehiveConfig Type, defined in beehive::net::config.
        let config: Config = toml::de::from_str(&contents).unwrap();
        config
    }
}
