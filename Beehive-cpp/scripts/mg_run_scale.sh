#!/bin/bash
set -e
pushd /path/to/FarLib/build
your_password="password"
ninja mg
core=16
cpu_core_start=24
local_mem_size_gb=4
program="./benchmark/mg/mg"
config="../mg.config"
result_dir="../results/mg/scale/record"
output_suffix="-scale"
mkdir -p ${result_dir}
local_mem_size=$[${local_mem_size_gb} * 1024 * 1024 * 1024]
sed "s/client_buffer_size.*/client_buffer_size ${local_mem_size}/g" ../mg.config -i
for qp_count in 1 2 4 8 16 24
do
    sed "s/.*qp_count.*/qp_count ${qp_count}/g" ${config} -i
    command="FibreCpuSet=${cpu_core_start}-$[${cpu_core_start} + ${core} - 1] nice -n -20 ${program} ${config}"
    output_file=${result_dir}/mg-${qp_count}qp${output_suffix}.txt
    echo "${command} > ${output_file}"
    echo your_password | sudo -S ${command} > ${output_file}
    sleep 30
done
