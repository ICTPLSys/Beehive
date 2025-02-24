SCRIPTS=$(dirname $0)
BUILD="$SCRIPTS/../build"
source $SCRIPTS/cgroup.sh

function build() {
    pushd $BUILD
    ninja
    popd
}

function run() {
    pushd $BUILD
    ./benchmark/bplus_tree_iteration ../config 1048576
    popd
}

build

initCgroup "rswap"
setCpuSet   "rswap" "0-79"
setNumaNode "rswap" "0"

setMemMaxMB "rswap" 20 90
setCgroup "rswap" $$
echo "running..."
run

run
