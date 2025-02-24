set -e
core_num=16
core_base=24
pushd /path/to/Beehive-cpp/build
ninja main
output_suffix="-scale"
result_dir="../results/dataframe/scale/record$1"
config="../dataframe.config"
mkdir -p ${result_dir}
data=/mnt/all.csv
local_mem_size=$[4 * 1024 * 1024 * 1024]
for qp_count in 1 2 4 8 16 24
do
    sed "s/.*qp_count.*/qp_count ${qp_count}/g" ${config} -i
    result_file="${result_dir}/${qp_count}qp${output_suffix}.txt"
    command="sudo FibreCpuSet=${core_base}-$[${core_base} + ${core_num} - 1] ./bin/main ${config} ${data} ${local_mem_size}"
    echo "$command > $result_file"
    echo your_password | sudo -S bash -c "$command > $result_file"
    sleep 30
done
popd