#!/bin/bash
set -e
pushd /path/to/FarLib/benchmark/socialNetwork/
your_password="password"
core=16
local_mem_size=6 # 12.5%, 25%, 50%, 75%, 100%
network_name="socfb-A-anon"
network_file="/path/to/social-network/${network_name}.mtx"
result_dir=../../results/social-network/mem125-scheduler
config_file="../../social_network.config"
mkdir -p ${result_dir}
# thread_nums=($[(${core} - 4) * 1] $[(${core} - 4) * 2] $[(${core} - 4) * 4] $[(${core} - 4) * 8])
thread_nums=(160 192 224 256)

program_name="socialNetwork"
for i in ${thread_nums[@]}
do
    sed "s/constexpr static uint64_t kNumServerThreads.*/constexpr static uint64_t kNumServerThreads = $[$i];/g" config.hpp -i
    pushd /path/to/FarLib/build
    ninja $program_name
    popd
    # output_file="./build/test.txt"
    output_file="${result_dir}/sn-${core}-${local_mem_size}g-profile-${i}th.txt"
    echo "sudo FibreCpuSet=24-$[24 + $core - 1] nice -n -20 stdbuf -o0 \
        ../../build/benchmark/socialNetwork/$program_name ${config_file} ${network_file} $[$local_mem_size * 1024 * 1024 * 1024]\
        1>$output_file"

    echo your_password | sudo -S FibreCpuSet=24-$[24 + $core - 1] nice -n -20 stdbuf -o0 \
        ../../build/benchmark/socialNetwork/$program_name ${config_file} ${network_file} $[$local_mem_size * 1024 * 1024 * 1024]\
        1>$output_file
    # exit
    sleep 30
done
popd
