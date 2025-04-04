use beehive::net::Config;
use beehive::net::Server;
use std::env;

fn main() {
    pretty_env_logger::init();
    let args: Vec<String> = env::args().collect();
    let path = &args[1];
    let config: Config = Config::load_config(path);
    let mut server = Server::new(config);
    server.connect();
}
