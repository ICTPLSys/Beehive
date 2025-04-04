#!/bin/bash
set -e
program_name="socialNetwork"
pushd /path/to/FarLib/benchmark/socialNetwork/
your_password="password"
core=16
# local_mem_size=(4 8 15 23 50) # 12.5%, 25%, 50%, 75%, 100%
local_mem_size=(4) # 12.5%, 25%, 50%, 75%, 100%
network_name="socfb-A-anon"
network_file="/path/to/social-network/${network_name}.mtx"
mode="throughput"
result_dir=../../results/social-network/latency
mkdir -p ${result_dir}
# for op_dur_t in 1us 2us 3us 4us 5us 6us 7us 8us 9us 10us
for op_dur_t in 20us 50us
do
    sed "s/.*op_duration = .*/.op_duration = ${op_dur_t},/g" latency_main.cpp -i
    pushd /path/to/FarLib/build
    ninja $program_name
    popd
    for i in ${local_mem_size[@]}
    do
        # output_file="./build/test.txt"
        output_file="${result_dir}/sn-${core}-${i}g-${mode}-${network_name}-${op_dur_t}.txt"
        echo "sudo FibreCpuSet=24-$[24 + $core - 1] nice -n -20 \
            ../../build/benchmark/socialNetwork/$program_name ../../social_network.config ${network_file} $[i * 1024 * 1024 * 1024]\
            1>$output_file"

        echo your_password | sudo -S FibreCpuSet=24-$[24 + $core - 1] nice -n -20 \
            ../../build/benchmark/socialNetwork/$program_name ../../social_network.config ${network_file} $[i * 1024 * 1024 * 1024]\
            1>$output_file
        # exit
        sleep 30
    done
done
popd
