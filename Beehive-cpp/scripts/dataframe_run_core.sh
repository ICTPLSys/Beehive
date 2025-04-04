set -e
core_num=16
core_base=24
pushd /path/to/FarLib/build
output_suffix="-drilldown"
result_dir="../results/dataframe/mem125-scheduler"
config="../dataframe.config"
your_password="password"
data_path="/path/to/dataframe/taxi.csv"
sed "s/max_thread_cnt.*/max_thread_cnt ${core_num}/g" ${config} -i
mkdir -p ${result_dir}
local_mem_size=$[4 * 1024 * 1024 * 1024]
sed "s/client_buffer_size.*/client_buffer_size  ${local_mem_size}/g" ${config} -i
# thread_nums=($[${core_num} * 1] $[${core_num} * 2] $[${core_num} * 4] $[${core_num} * 8])
thread_nums=(160 192 224 256)
for thread_num in ${thread_nums[@]}    # 12.5%, 25%, 50%, 75%
do
    sed "s/inline size_t get_thread_count().*/inline size_t get_thread_count() { return ${thread_num}; }/g" ../include/utils/uthreads.hpp -i
    ninja main
    result_file="${result_dir}/${thread_num}th${output_suffix}.txt"
    command="sudo FibreCpuSet=${core_base}-$[${core_base} + ${core_num} - 1] ./bin/main ${config} ${data_path} ${local_mem_size}"
    echo "$command > $result_file"
    echo ${your_password} | sudo -S bash -c "$command > $result_file"
    sleep 30
done
popd