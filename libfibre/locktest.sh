#!/usr/bin/env bash

# trylock:timedlock:lockname
TYPES=(
  "1:0:MCSLock"
  "1:0:TicketLock"
  "1:0:BinaryLock<>"
  "1:0:SimpleMutex0<true>"
  "1:0:SimpleMutex0<false>"
  "1:1:LockedMutex<WorkerLock, true>"
  "1:1:LockedMutex<WorkerLock, false>"
  "1:1:FredMutex"
  "1:0:FastMutex"
  "1:1:OwnerMutex<FredMutex>"
  "1:0:OwnerMutex<FastMutex>"
)

# trylock:timedlock:locktype
SUBTYPES=(
  "0:0:B"
  "1:0:S"
  "1:0:Y"
  "0:1:T"
)

function pre_MCSLock() {
  sed -i -e "/static void worker(void\* arg) {/a \ \ MCSLock::Node node;" apps/threadtest.cpp
  sed -i -e "s/\(.*shim_mutex_lock([^)]*\)\().*\)/\1, node\2/" apps/threadtest.cpp
  sed -i -e "s/\(.*shim_mutex_trylock([^)]*\)\().*\)/\1, node\2/" apps/threadtest.cpp
  sed -i -e "s/\(.*shim_mutex_unlock([^)]*\)\().*\)/\1, node\2/" apps/threadtest.cpp
  sed -i -e "s/\(.*shim_cond_wait.*{\).*\(}\)/\1\2/" apps/include/libfibre.h
}

function cleanup() {
	make clean > compile.out
	rm -f compile.out
  git checkout apps
  exit 1
}

trap cleanup SIGHUP SIGINT SIGQUIT SIGTERM

for t in "${!TYPES[@]}"; do
	MUTEXLOCK=$(echo ${TYPES[$t]}|cut -f3 -d:)
	HASTRYLOCK=$(echo ${TYPES[$t]}|cut -f1 -d:)
	HASTIMEDLOCK=$(echo ${TYPES[$t]}|cut -f2 -d:)
  sed -i -e "s/typedef FredMutex shim_mutex_t;/typedef ${MUTEXLOCK} shim_mutex_t;/" apps/include/libfibre.h
  sed -i -e "s/#define HASTRYLOCK 1/#define HASTRYLOCK ${HASTRYLOCK}/" apps/include/libfibre.h
  sed -i -e "s/#define HASTIMEDLOCK 1/#define HASTIMEDLOCK ${HASTIMEDLOCK}/" apps/include/libfibre.h
  type pre_${MUTEXLOCK} &>/dev/null && pre_${MUTEXLOCK}
	echo "========== Compiling ${MUTEXLOCK} =========="
  make clean > compile.out
  make -j $(nproc) -C apps threadtest >> compile.out
  for s in "${!SUBTYPES[@]}"; do
		LOCKTYPE=$(echo ${SUBTYPES[$s]}|cut -f3 -d:)
  	REQTRYLOCK=$(echo ${SUBTYPES[$s]}|cut -f1 -d:)
		REQTIMEDLOCK=$(echo ${SUBTYPES[$s]}|cut -f2 -d:)
	  [ "${REQTRYLOCK}" -le "${HASTRYLOCK}" ] && [ "${REQTIMEDLOCK}" -le "${HASTIMEDLOCK}" ] && {
      echo "========== Testing ${LOCKTYPE} =========="
      ./apps/threadtest -d 60 -f 1024 -t 32 -l 4 -w 1000 -u 1000 -L ${LOCKTYPE} || exit 1
    }
  done
  rm -f compile.out
  git checkout apps
done
exit 0

# debug run:
while gdb -q\
 -ex "set print thread-events off"\
 -ex "set pagination off"\
 -ex "break _SYSCALLabort"\
 -ex "run -l8 -t32 -f1024 -w1 -u1"\
 -ex "quit" ./apps/threadtest; do sleep 3;
done
