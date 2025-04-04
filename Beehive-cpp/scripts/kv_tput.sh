set -e
for mem in 7.5
do
    echo "Running, local memory = $mem GiB, log at ./results/kvs/$mem.log"
    FibreCpuSet=0-15 build/benchmark/kvs/kvs_service_throuput kv.config $mem | tee results/kvs/$mem.log
done

