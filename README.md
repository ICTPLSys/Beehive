# 1. Summary of Beehive

Beehive is a memory-disaggregated framework that improves remote access throughput by exploiting the asynchrony within each thread. It allows programmers to develop applications in the conventional multithreaded synchronous model. It automatically transforms the code into *pararoutine* (a newly proposed computation and scheduling unit) based asynchronous code via the Rust compiler.

# 2. Setup Environment

 You will need at least two servers with RDMA connections to use Beehive. One will serve as the client and the others as the memory servers. Although Beehive Beehive is developed and tested under the following.

- Hardware：
  - Infiniband: Mellanox ConnectX-5 (100Gbps)
- Software：
  - OS: Ubuntu 20.04
  - GCC: 13.1
  - Mellanox OFED driver: MLNX_OFED_LINUX-5.8-6.0.4.2/MLNX_OFED_LINUX-4.9-5.1.0.0

 In theory, there are no special requirements for operating system and Mellanox driver versions, but we recommend that you follow the above scheme to avoid potential problems. 

# 3. Build and Run

We have the C++ version and Rust version of Beehive. 

## 3.1 Preparations

​	Before building Beehive, we have some preparation work to do. (Unless otherwise specified, the following steps must be performed on both the client and server)

### Kernel Setting

- **Enable RDMA**: Please confirm the RDMA works well between your client and server, or you should follow the following steps to enable RDMA.

  - Install MLNX-OFED driver.

    ```bash
    # https://network.nvidia.com/products/infiniband-drivers/linux/mlnx_ofed/
    wget https://content.mellanox.com/ofed/MLNX_OFED-5.8-6.0.4.2/MLNX_OFED_LINUX-5.8-6.0.4.2-ubuntu20.04-x86_64.tgz
    tar xzf MLNX_OFED_LINUX-5.8-6.0.4.2-ubuntu20.04-x86_64.tgz
    cd MLNX_OFED_LINUX-5.8-6.0.4.2-ubuntu20.04-x86_64.tgz
    sudo ./mlnxofedinstall --add-kernel-support
    sudo /etc/init.d/openibd restart
    ```

  - check the connection works well.

    ```bash
    # client
    rping -c -v -a <server rdma ip> -C 10
    # server
    rping -s
    ```

- **Hugepage Setting**: Please confirm the total memory size of hugepage is large enough for the beehive.

  - for the server, the size should be larger than the remote memory size you configured in the config file.
  - for the client, the size should be larger than the local memory size you configured in the config file.

  ```bash
  hugepage_size_2MB=<user configured size>
  sudo sh -c 'echo $hugepage_size_2MB > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages'
  ```

### Library

 Beehive relies on several third-party libraries that need to be installed in advance. 

- PAPI(for Beehive C++ and Rust): https://github.com/icl-utk-edu/papi. 
  Follow the README.md to install PAPI. Beehive uses PAPI to profile. We recommend installing PAPI on the root directory. 
- Fred / Libfibre(for Beehive C++ and Rust): we fork Fred from https://gitlab.uwaterloo.ca/k49sun/libfibre and make some modification to fit Beehive. Please install the Fred library from `./libfibre` according to its `README.md`.
- HdrHistogram(for Beehive C++): https://github.com/HdrHistogram/HdrHistogram. 
  Follow the README.md to install HdrHistogram. 
- cpp-jwt(for Beehive C++): https://github.com/arun11299/cpp-jwt.
  Follow the README.md to install cpp-jwt. Beehive uses it in some benchmarks. 

## 3.2 Beehive C++

### Build System

We use cmake as our build system. So firstly, you need to install cmake if you don't have one. 
```bash
# cmake version >= 3.25
sudo apt install cmake
```

### Compile

```bash
cd /path/to/beehive-cpp/
mkdir build
cd build
# ninja is not 
cmake ..
```

### Configuration

Here is an example configuration file for Beehive C++. You can refer to the comments to write your configuration file. 

```config
### Configuration format:
### <configuration Item>	<value>
server_addr             10.208.130.30
server_port             50000
server_buffer_size  	83886080	# remote memory size(bytes)
client_buffer_size  	8388608		# local memory size(bytes)
# Evacuators try to pack multiple small evacuation requests into one large request to send out at once
evict_batch_size        65536		# batch size for evacuator
qp_count                16			# qp count for 1 thread
max_thread_cnt      16				# worker CPUs for Beehive
evacuate_thread_cnt 128				# Evacuator threads for Beehive
# qp_send_cap           8
# qp_recv_cap           16
# other configurations for RDMA connections...
```

### Run

 You can execute programs directly with commands or with our scripts. If you want to use scripts, remember to change some of the script's variables from preset values to values that match your environment.
  ```bash
  # server
  cd /path/to/beehive-cpp/build/
  ./server /path/to/server/config
  # client
  cd /path/to/beehive-cpp/scripts
  bash xx.sh  # scripts in /path/to/beehive-cpp/scripts
  # or you can execute it directly 
  cd /path/to/beehive-cpp/build
  /path/to/app/binary <config file> [other args]
  # When your arguments are incorrect, a help message will be printed on the screen telling you the correct format for the arguments
  ```

## 3.3 Beehive Rust

 We use Rust 1.86.0-nightly to write our Rust Beehive. We recommend that you install this version or higher of Rust.

### Build System

If you don't have Cargo, you can download it at https://www.rust-lang.org/tools/install and follow its steps to install it.

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Beehive uses Nightly Rust, so check your Rust version.

```bash
cargo --version
```

If it's not a Nightly version, install one by the following commands.

```bash
rustup install nightly
rustup default nightly
```

### Compile

Before Compiling. You should install the necessary libraries as described in the *library* section.

```bash
cd /path/to/beehive-rust
# rust compile
cargo check
cargo build
```

### Configuration

Here is an example configuration file for Beehive Rust(usually `*.toml`). You can refer to the comments to write your own configuration file. 

```toml
# RDMA configuration
cq_entries = 32768
qp_send_cap = 256
qp_recv_cap = 64
qp_max_send_sge = 1
qp_max_recv_sge = 1
qp_max_rd_atomic = 16
qp_mtu = 5
qp_min_rnr_timer = 12
qp_timeout = 8
qp_retry_cnt = 7
qp_rnr_retry = 7
ib_port = 1

# client configuration
client_memory_size = 16384000	# local memory size
evict_batch_size = 262144		# batch size for evacuator
poll_cq_batch_size = 32			# wc buffer size for polling cq
num_cores = 1					# worker CPUs for Beehive
num_evacuate_threads = 1		# evacuator threads for Beehives

# remote server configuration
servers = [
    { addr = "127.0.0.1:8888", memory_size = 4294967296 },
]
```

### Test


Unfortunately, there is a conflict between Fred and the cargo's test suite. So we have to divide tests into two parts. You need to manually run the tests that use Fred or remote memory.
```bash
# Tests without Fred & remote memory
cargo test
# Tests with Fred or remote memory
# server
cargo run --bin server <server config file> # e.g. server.toml
# client
cargo run --bin <test> <client config file> # e.g. client.toml
```

## 3.4 Data

​	Use https://www.kaggle.com/datasets/fishlifehh/beehive-benchmark-datasets to download the data you need to run benchmarks.

# FAQ

​	If you encounter any problems, please open an issue or feel free to contact us. 