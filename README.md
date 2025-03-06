# Beehive

Beehive is a memory disaggregated framework which improves the remote access throughput by exploiting the asynchrony within each thread. It allows the programmers to develop applications in the conventional multithreaded synchronous model and automatically transforms the code into *pararoutine* (a newly proposed computation and scheduling unit) based asynchronous code via the Rust compiler.

*Under Construction:)*

## Setup

### Environment

​	You will need at least two servers with RDMA connections to use Beehive. One will serve as the client and others will serve as the memory servers. Although Beehive Beehive is developed and tested under the following.

- Hardware：
  - Infiniband: Mellanox ConnectX-5 (100Gbps)
- Software：
  - OS: Ubuntu 20.04
  - GCC: 13.1
  - Mellanox OFED driver: MLNX_OFED_LINUX-5.8-6.0.4.2/MLNX_OFED_LINUX-4.9-5.1.0.0

​	In theory, there are no special requirements for operating system and Mellanox driver versions, but we recommend that you follow the above scheme to avoid potential problems. 

### Library

​	Beehive relies on a number of third-party libraries that need to be installed in advance. 

- PAPI: https://github.com/icl-utk-edu/papi. 
  Follow the README.md to install PAPI. Beehive use PAPI to profile. We recommend installing PAPI on the root directory. 
- Fred / Libfibre: we fork Fred from its origin https://gitlab.uwaterloo.ca/k49sun/libfibre and make some modification to fit Beehive. Follow the README.md to install Fred.
- HdrHistogram: https://github.com/HdrHistogram/HdrHistogram. 
  Follow the README.md to install HdrHistogram. 
- cpp-jwt: https://github.com/arun11299/cpp-jwt.
  Follow the README.md to install cpp-jwt. Beehive use it in some benchmarks. 

## Build and Run

​	We prepare C++ version and Rust version Beehive. 

### Beehive C++

- **build system**

  We use cmake as our build system. So firstly, you need to install cmake if you don't have one. 

  ```bash
  # cmake version >= 3.25
  sudo apt install cmake
  ```

- **compile**

  ```bash
  cd /path/to/beehive-cpp/
  mkdir build
  cd build
  # ninja is not 
  cmake ..
  ```
  
- **run**

  ```bash
  # server
  cd /path/to/beehive-cpp/build/
  ./server /path/to/server/config
  # client
  cd /path/to/beehive-cpp/scripts
  bash xx.sh	# scripts in /path/to/beehive-cpp/scripts
  ```

### Beehive Rust

​	We use Rust 1.86.0-nightly to write our Rust Beehive. We recommend that you install this version or higher of Rust.

​	Thanks to Cargo, Rust compile and run is simple. 

```bash
cd /path/to/beehive-rust
# rust compile
cargo build
# rust test
cargo test
# rust run
# server
cargo run --bin server /path/to/server/config
# client
cargo run --bin <program name> [args...]
```

## Paper

wait for link.