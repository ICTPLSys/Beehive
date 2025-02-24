#!/bin/bash
set -e
pushd /path/to/Beehive-cpp/benchmark/llama
core=16
config_file="../../llama.config"
sed "s/max_thread_cnt.*/max_thread_cnt ${core}/g" ${config_file} -i
sed "s/evacuate_thread_cnt.*/evacuate_thread_cnt $[${core} / 4]/g" ${config_file} -i
thread_num=($[1 * ${core}] $[2 * ${core}] $[4 * ${core}] $[8 * ${core}])
# local_mem_size=(3 6 13 19 72) # 12.5%, 25%, 50%, 75%, 100%
local_mem_size=3 # 12.5%, 25%, 50%, 75%, 100%
result_dir="../../results/llama/mem125"
mkdir -p $result_dir
input_file="./llama_user_chat.txt"
llama_bin="/mnt/llama/llama2_7b_chat.bin"
output_suffix="-drilldown"
program_name="run_chat_far"
for i in ${thread_num[@]}
do
    sed "s/static inline size_t get_thread_count().*/static inline size_t get_thread_count() { return ${i}; }/g" ${config_file} -i
    pushd /path/to/Beehive-cpp/build
    ninja $program_name
    popd
    output_file="$result_dir/chat-${local_mem_size}g-${i}th${output_suffix}.txt"
    command="FibreCpuSet=24-$[24 + $core - 1] nice -n -20 \
        ../../build/benchmark/llama/$program_name ../../llama.config $llama_bin\
        -m chat -b $[$local_mem_size * 1024 * 1024 * 1024]"
    echo "${command} < ${input_file} 1>${output_file}"

    echo your_password | sudo -S bash -c "${command} < ${input_file} 1>${output_file}"
    # exit
    sleep 30
done
popd
