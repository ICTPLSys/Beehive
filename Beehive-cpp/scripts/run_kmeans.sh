set -e

SCRIPTS=$(dirname $0)
BUILD="$SCRIPTS/../build"
source $SCRIPTS/cgroup.sh

function build() {
    pushd $BUILD
    ninja ./benchmark/kmeans
    popd
}

function run() {
    pushd $BUILD
    for POINT_M in 1 2 4 8 16 32 64
    do
        taskset -c 0 ./benchmark/kmeans ../kmeans.config 16 $[1024 * 1024 * $POINT_M] 2 $[1024 * 1024 * $POINT_M * 4] | grep "iteration #1"
    done
    popd
}

build

    run
