#!/bin/bash

# 12GB for total
# 
data_path=/path/to/ligra_rMat_100M
for mem in 12 9 6 3 1.5 
do
    echo "Running, local memory = $mem GiB, log at ./results/ligra/cc/$mem.log"
    FibreCpuSet=8-23 ./build/benchmark/graph/cc -c ./graph.config -M $mem -i $data_path -v 1 -n 10 > ./results/ligra/cc/$mem.log
done

