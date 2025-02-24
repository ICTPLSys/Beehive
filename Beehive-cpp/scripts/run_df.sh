set -e
test_uth_num=()
core_num=4
# test_uth_num=(35 40 53 55 56 63)
for ((i = 4;i <= 256;i += (i < 16 ? 1 : (i < 64 ? core_num : i))))
do
    test_uth_num+=($i)
done
# test_uth_num=(4 5 6 7 8 12 16 32 64 128 256)
pushd /path/to/mem_parallel/motivation/Beehive-cpp/build

cpuset='0-'
cpuset+=$[$core_num - 1]
# for ((k = 1;k < 4;k++))
# do
#     echo "------------------------ test $k ----------------------------"
#     for uth_num in ${test_uth_num[@]}
#     do
#         command="sudo FibreCpuSet=$cpuset nice -n -20 ./bin/motivation ../df-config $core_num $uth_num"
#         output="trip-duration-uthread-${core_num}-${uth_num}-lim-wo-out-${k}.txt"
#         echo "$command > $output"
#         $command > $output
#         sleep 10
#     done
# done

# for ((k = 0;k < 3;k++))
# do
    echo "------------------------ test $k ----------------------------"
    for uth_num in ${test_uth_num[@]}
    do
        pushd /path/to/mem_parallel/motivation/Beehive-cpp/include/
        echo -e "#pragma once\nconstexpr size_t DEFAULT_BATCH_SIZE = $uth_num;" > inline_task_option.hpp
        popd
        ninja motivation
        command="sudo FibreCpuSet=$cpuset nice -n -20 ./bin/motivation ../df-config $core_num $core_num"
        # output="trip-duration-pararoutine-${core_num}-${uth_num}-lim-wo-out-${k}.txt"
        echo "$command"
        $command
        sleep 5
    done
# done
popd