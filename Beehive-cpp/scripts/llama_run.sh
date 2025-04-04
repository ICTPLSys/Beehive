#!/bin/bash
set -e
pushd /path/to/FarLib/build
program_name="run_chat_far"
ninja $program_name
popd
pushd /path/to/FarLib/benchmark/llama
your_password="password"
core=16
# local_mem_size=(3 6 13 19 72) # 12.5%, 25%, 50%, 75%, 100%
local_mem_size=(72) # 12.5%, 25%, 50%, 75%, 100%
result_dir="../../results/llama/${core}core"
mkdir -p $result_dir
input_file="/path/to/llama/llama_user_chat.txt"
llama_bin="/path/to/llama/llama2_7b_chat.bin"
output_suffix="-drilldown-profile-prefetch-2"
for i in ${local_mem_size[@]}
do
    # output_file="./build/test.txt"
    output_file="$result_dir/chat-${core}-${i}g.txt"
    command="FibreCpuSet=24-$[24 + $core - 1] nice -n -20 \
        ../../build/benchmark/llama/$program_name ../../llama.config $llama_bin\
        -m chat -b $[$i * 1024 * 1024 * 1024]"
    echo "${command} < ${input_file} 1>${output_file}"

    echo your_password | sudo -S bash -c "${command} < ${input_file} 1>${output_file}"
    # exit
    sleep 30
done
popd
