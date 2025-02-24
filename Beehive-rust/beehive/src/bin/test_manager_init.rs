use beehive::mem::manager;
use beehive::net::Config;
use std::env;

fn main() {
    pretty_env_logger::init();
    let args: Vec<String> = env::args().collect();
    let path = &args[1];
    let config = Config::load_config(path);
    manager::initialize(config);
    manager::destroy();
}
