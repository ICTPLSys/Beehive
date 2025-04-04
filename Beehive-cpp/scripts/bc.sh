#!/bin/bash

set -e

# 16GB for total
# 
data_path=/path/to/ligra_rMat_50M
for mem in 2 4 8.5 12 32
do
    echo "Running, local memory = $mem GiB, log at ./results/ligra/bc/$mem.log"
    FibreCpuSet=8-23 ./build/benchmark/graph/bc -c ./graph.config -M $mem -i $data_path -v 1 -n 16 | tee ./results/ligra/bc/$mem.log
    echo
done

