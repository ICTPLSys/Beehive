#!/bin/bash
set -e
pushd /path/to/Beehive-cpp/build
core=16
cpu_core_start=24
local_mem_size_gb=4
program="./benchmark/mg/mg"
config="../mg.config"
result_dir="../results/mg/drilldown"
output_suffix="-drilldown"
mkdir -p ${result_dir}
# thread_nums=($[${core} * 1] $[${core} * 2] $[${core} * 4] $[${core} * 8])
thread_nums=(256 192 160 224)
local_mem_size=$[${local_mem_size_gb} * 1024 * 1024 * 1024]
sed "s/client_buffer_size.*/client_buffer_size ${local_mem_size}/g" ../mg.config -i
for thread_num in ${thread_nums[@]}
do
    sed "s/constexpr size_t UthreadCount =.*/constexpr size_t UthreadCount = ${thread_num};/g" ../benchmark/mg/mg.cpp -i    
    ninja mg
    command="FibreCpuSet=${cpu_core_start}-$[${cpu_core_start} + ${core} - 1] nice -n -20 ${program} ${config}"
    output_file=${result_dir}/mg-${core}core-${thread_num}th${output_suffix}.txt
    echo "${command} > ${output_file}"
    echo your_password | sudo -S ${command} > ${output_file}
    sleep 30
done
