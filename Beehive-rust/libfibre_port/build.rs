use std::env;
use std::path::PathBuf;

fn main() {
    let libfibre_path = PathBuf::from(env::var("LIBFIBRE_DIR").unwrap()).join("src");
    cc::Build::new()
        .file("../Cfibre/cfibre.cpp")
        .include(libfibre_path.as_path())
        .opt_level(3)
        .compile("libcfibre.a");
    let libfibre_dir = std::env::var("LIBFIBRE_DIR").unwrap();
    assert!(libfibre_dir.len() > 0);
    let libfibre_path = libfibre_dir + "/src";
    let libdirs: [&str; 2] = [&libfibre_path, "/lib/gcc/x86_64-linux-gnu/13"];
    for libdir in libdirs {
        println!("cargo:rustc-link-search={}", libdir);
    }
    println!("cargo:rustc-link-lib=static=cfibre");
    println!("cargo:rustc-link-lib=static=fibre");
    println!("cargo:rustc-flags=-l dylib=stdc++");
}
