#!/bin/bash

# utils for setup cgroups and cpu / memory limit

CG_V="V1"
CGROUP=/sys/fs/cgroup

source $SCRIPTS/password.sh

initCgroup() {
    echo "Initializing cgroups"
    if [ "$CG_V" == "V1" ]; then
    echo $PASSWD | sudo -S -p "" mkdir -p $CGROUP/memory/$1
    echo $PASSWD | sudo -S -p "" mkdir -p $CGROUP/cpuset/$1
    elif [ "$CG_V" == "V2" ]; then
    echo $PASSWD | sudo -S -p "" -S sh -c "echo '+cpuset +memory' > ${CGROUP}/cgroup.subtree_control"
    echo $PASSWD | sudo -S -p "" mkdir -p $CGROUP/$1
    else
    echo "Failed to setup Cgroup"
    exit 1
    fi
}

setCpuSet(){
  # $1: group
  # $2: cpu ids

  echo "Set up cpu set for $1: $2"
  if [ "$CG_V" == "V1" ]; then
    cgcpus=$CGROUP/cpuset/$1/cpuset.cpus
  else
    cgcpus=$CGROUP/$1/cpuset.cpus
  fi
  echo $PASSWD | sudo -S -p "" -S sh -c "echo $2 > $cgcpus"
}

setNumaNode(){
  # $1: group
  # $2: mem node

  echo "Set up numa node for $1: $2"

  if [ "$CG_V" == "V1" ]; then
    cgmems=$CGROUP/cpuset/$1/cpuset.mems
  else
    cgmems=$CGROUP/$1/cpuset.mems
  fi
  echo $PASSWD | sudo -S -p "" -S sh -c "echo $2 > $cgmems"
}

setMemMax(){
  # $1: group
  # $2: mem in GB
  # $3: mem ratio
  setMemMaxMB $1 $[ $2 * 1024 ] $3
}

setMemMaxMB(){
  # $1: group
  # $2: mem in GB
  # $3: mem ratio

  case "${3}${CG_V}" in
    "100V1")
      memmax_byte=-1
    ;;
    "100V2")
      memmax_byte='max'
    ;;
    *)
      memmax_byte=$[ $2 * 1024 * 1024 * $3 / 100 ]
    ;;
  esac
  if [ "$CG_V" = "V1" ]; then
    cgmem=$CGROUP/memory/$1/memory.limit_in_bytes
  else
    cgmem=$CGROUP/$1/memory.max
  fi
  echo $PASSWD | sudo -S -p "" -S sh -c "echo $memmax_byte > $cgmem"
}

setCgroup() {
  # $1: group
  # $2: proc pid

  if [ "$CG_V" == "V1" ]; then
    # echo $PASSWD | sudo -S sh -c "echo $2 > $CGROUP/cpuset/$1/cgroup.procs"
    echo $PASSWD | sudo -S sh -c "echo $2 > $CGROUP/memory/$1/cgroup.procs"
  else
    echo $PASSWD | sudo -S -p "" -S sh -c "echo $2 > $CGROUP/$1/cgroup.procs"
  fi
}

execInCgroup() {
  # $1: group
  # $2: command
  export -f setCgroup
  CG_V=$CG_V \
  CGROUP=$CGROUP \
  PASSWD=$PASSWD \
  bash -c "setCgroup $1 \$\$; $2"
}
