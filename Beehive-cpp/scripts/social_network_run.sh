#!/bin/bash
set -e
pushd /path/to/FarLib/build
program_name="socialNetwork"
ninja $program_name
popd
pushd /path/to/FarLib/benchmark/socialNetwork/
your_password="password"
core=16
# local_mem_size=(4 8 15 23 50) # 12.5%, 25%, 50%, 75%, 100%
local_mem_size=(4) # 12.5%, 25%, 50%, 75%, 100%
network_name="socfb-A-anon"
network_file="/path/to/social-network/${network_name}.mtx"
mode="throughput"
result_dir=../../results/social-network/${core}core
mkdir -p ${result_dir}
for i in ${local_mem_size[@]}
do
    # output_file="./build/test.txt"
    output_file="${result_dir}/sn-${core}-${i}g-${mode}-${network_name}-profile.txt"
    echo "sudo FibreCpuSet=24-$[24 + $core - 1] nice -n -20 \
        ../../build/benchmark/socialNetwork/$program_name ../../social_network.config ${network_file} $[i * 1024 * 1024 * 1024]\
        1>$output_file"

    echo your_password | sudo -S FibreCpuSet=24-$[24 + $core - 1] nice -n -20 \
        ../../build/benchmark/socialNetwork/$program_name ../../social_network.config ${network_file} $[i * 1024 * 1024 * 1024]\
        1>$output_file
    # exit
    sleep 30
done
popd
