use proc_macro::TokenStream;
use quote::quote;
use syn::{AttributeArgs, ItemFn, NestedMeta, parse_macro_input};

#[proc_macro_attribute]
pub fn beehive_main(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(input as ItemFn);
    let fn_block = &input_fn.block;

    let result = quote! {
        fn main() {
            pretty_env_logger::init();
            let args: Vec<String> = std::env::args().collect();
            let path = &args[1];
            let config = beehive::net::Config::load_config(path);
            beehive::initialize(&config);
            log::info!("init done!");
            #fn_block;
            beehive::destroy();
            log::info!("beehive exit!");
        }
    };
    result.into()
}

#[proc_macro_attribute]
pub fn beehive_test(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);
    let client_config_path = match &args[0] {
        NestedMeta::Lit(syn::Lit::Str(lit_str)) => lit_str.value(),
        _ => panic!("Expected a string literal as the first argument"),
    };
    let server_config_path = match &args[1] {
        NestedMeta::Lit(syn::Lit::Str(lit_str)) => lit_str.value(),
        _ => panic!("Expected a string literal as the second argument"),
    };
    let input_fn = parse_macro_input!(input as ItemFn);
    let fn_block = &input_fn.block;
    let fn_name = input_fn.sig.ident;
    let result = quote! {
        #[test]
        #[serial]
        fn #fn_name() {
            pretty_env_logger::init();
            let server_config_path = #server_config_path;
            let client_config_path = #client_config_path;
            let server_config = crate::net::Config::load_config(server_config_path);
            let handle = std::thread::spawn(|| {
                let mut server = crate::net::Server::new(server_config);
                server.connect();
            });
            std::thread::sleep(std::time::Duration::from_secs(2));
            let client_config = crate::net::Config::load_config(client_config_path);
            crate::initialize(&client_config);
            #fn_block
            crate::destroy();
        }
    };
    result.into()
}
