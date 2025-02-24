use std::env;
use std::path::PathBuf;

use bindgen::Bindings;

fn ib_build() {
    let wrapper_header = "src/net/bindings/wrapper.h";
    let wrapper_source = "src/net/bindings/wrapper.c";

    println!("cargo:rerun-if-changed={wrapper_header}");
    println!("cargo:rerun-if-changed={wrapper_source}");
    println!("cargo:rustc-link-lib=ibverbs");

    let mut builder = bindgen::Builder::default()
        .header(wrapper_header)
        .bitfield_enum("ib_uverbs_access_flags")
        .bitfield_enum("ibv_.*_bits")
        .bitfield_enum("ibv_.*_caps")
        .bitfield_enum("ibv_.*_flags")
        .bitfield_enum("ibv_.*_mask")
        .newtype_enum("ibv_qp_type")
        .newtype_enum("ibv_qp_state")
        .newtype_enum("ibv_.*_status")
        .newtype_enum("ibv_.*_opcode")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));

    for name in [
        "ibv_srq",
        "ibv_wq",
        "ibv_qp",
        "ibv_cq",
        "ibv_cq_ex",
        "ibv_context",
    ] {
        builder = builder.no_copy(name).no_debug(name)
    }

    let bindings = builder.generate().expect("Unable to generate bindings");
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    bindings
        .write_to_file(out_path.join("ib_bindings.rs"))
        .expect("Couldn't write bindings!");

    cc::Build::new()
        .file(wrapper_source)
        .compile("ib_bingdings.a");
}

fn papi_build() {
    let wrapper_header = "src/profile/bindings/wrapper.h";

    println!("cargo:rerun-if-changed={wrapper_header}");
    println!("cargo:rustc-link-lib=papi");
    println!("cargo:rustc-link-search=/usr/local/lib");

    let builder = bindgen::Builder::default()
        .header(wrapper_header)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));

    let bindings: Bindings = builder.generate().expect("cant generate papi lib");
    let out_dir: PathBuf = PathBuf::from(env::var("OUT_DIR").unwrap());

    bindings
        .write_to_file(out_dir.join("papi_bindings.rs"))
        .expect("cant write to binding file");
}

fn main() {
    ib_build();
    papi_build();
}
