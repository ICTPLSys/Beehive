#!/bin/bash
set -e
pushd /path/to/Beehive-cpp/build
ninja mg
core=16
cpu_core_start=24
# local_mem_size_gb=(4 9 17 25 64)
local_mem_size_gb=(64)
program="./benchmark/mg/mg"
config="../mg.config"
result_dir="../results/mg"
output_suffix="-wol3"
mkdir -p ${result_dir}
for i in ${local_mem_size_gb[@]}
do
    local_mem_size=$[${i} * 1024 * 1024 * 1024]
    sed "s/client_buffer_size.*/client_buffer_size ${local_mem_size}/g" ../mg.config -i
    command="FibreCpuSet=${cpu_core_start}-$[${cpu_core_start} + ${core} - 1] nice -n -20 ${program} ${config}"
    output_file=${result_dir}/mg-${core}core-${i}g${output_suffix}.txt
    echo "${command} > ${output_file}"
    echo your_password | sudo -S ${command} > ${output_file}
    sleep 30
done
