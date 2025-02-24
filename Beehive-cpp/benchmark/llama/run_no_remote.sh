pushd /path/to/Beehive/benchmark/llama
set -e
make clean
make runomp
core=16
user_chat="./build/llama_user_chat.txt"
# all_local_command="OMP_NUM_THREADS=${core} ./run ./build/llama2_7b_chat.bin -m chat"
# all_local_file="./build/${core}core/chat-${core}-all-local.txt"
# echo "$all_local_command < ${user_chat} > $all_local_file"
# bash -c "${all_local_command} < ${user_chat} > ${all_local_file}"
# exit
mkdir -p ./build/${core}core
sudo cgcreate -g memory:mem_limit_fishlife
for i in 3 6 13 19 # 12.5%, 25%, 50%, 75%
do
    command="sudo -S OMP_NUM_THREADS=${core} cgexec -g memory:mem_limit_fishlife --sticky ./run /mnt/ssd/data/llama2_7b_chat.bin \
        -m chat"
    output_file="./build/${core}core/chat-${core}-${i}g-time.txt"
    # echo "${command} < ${user_chat} > ${output_file}"
    echo ${command}
    sudo cgset -r memory.limit_in_bytes=${i}G mem_limit_fishlife

    echo huanghong | ${command} < ${user_chat} > ${output_file}
done
sudo cgdelete -g memory:mem_limit_fishlife
make clean
popd
