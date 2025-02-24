set -e
core_num=16
core_base=24
pushd /path/to/Beehive-cpp/build
ninja main
output_suffix="-profile"
result_dir="../results/dataframe/${core_num}core"
config="../dataframe.config"
mkdir -p ${result_dir}
for i in 4 8 15 23 72    # 12.5%, 25%, 50%, 75%
# for i in 4    # 12.5%, 25%, 50%, 75%
do
    local_mem_size=$[${i} * 1024 * 1024 * 1024]
    sed "s/client_buffer_size.*/client_buffer_size  ${local_mem_size}/g" ${config} -i
    result_file="${result_dir}/${i}g${output_suffix}.txt"
    command="sudo FibreCpuSet=${core_base}-$[${core_base} + ${core_num} - 1] ./bin/main ${config} /mnt/data2/all.csv ${local_mem_size}"
    echo "$command > $result_file"
    echo your_password | sudo -S bash -c "$command > $result_file"
    sleep 30
done
popd