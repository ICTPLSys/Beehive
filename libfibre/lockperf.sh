#!/usr/bin/env bash

TYPES=(
 "p:pthread:FredMutex"
    "f:cond:SpinCondMutex<ConditionalQueue<WorkerLock>, 0, 0, 0, 0>"
   "f:scond:SpinCondMutex<ConditionalQueue<WorkerLock>, 4, 1024, 16, 0, PauseSpin>"
    "f:qond:SpinCondMutex<ConditionalNemesisQueue<WorkerLock>, 0, 0, 0, 0>"
   "f:sqond:SpinCondMutex<ConditionalNemesisQueue<WorkerLock>, 4, 1024, 16, 0, PauseSpin>"
   "f:fibre:SpinSemMutex<FredBenaphore<LockedSemaphore<WorkerLock>,true>, 0, 0, 0, 0>"
  "f:sfibre:SpinSemMutex<FredBenaphore<LockedSemaphore<WorkerLock>,true>, 4, 1024, 16, 0, PauseSpin>"
    "f:fast:SpinSemMutex<FredBenaphore<LimitedSemaphore0<WorkerLock>,true>, 0, 0, 0, 0>"
   "f:sfast:SpinSemMutex<FredBenaphore<LimitedSemaphore0<WorkerLock>,true>, 4, 1024, 16, 0, PauseSpin>"
 "f:timeout:SpinSemMutex<MCSTimeoutSemaphore<MCSLock>, 0, 0, 0, 0>"
"f:stimeout:SpinSemMutex<MCSTimeoutSemaphore<MCSLock>, 4, 1024, 16, 0>"
#  "f:fifo:LockedMutex<WorkerLock, true>"
#  "f:simple:SimpleMutex0<false>"
#  "f:direct:SimpleMutex0<true>"
)

function cleanup() {
	make clean > compile.out
	rm -f compile.out perf.out run.out
  sed -i -e "s/typedef .* shim_mutex_t; \/\/ test/typedef FredMutex shim_mutex_t;/" apps/include/libfibre.h
  exit 1
}

trap cleanup SIGHUP SIGINT SIGQUIT SIGTERM

show=false
host=$(hostname)
if ! [ $1 -eq $1 ] 2>/dev/null; then
	show=true
	[ "$1" = "show" ] || host=$1
	shift
fi

for lcnt in $*; do
	if ! [ $lcnt -eq $lcnt ] 2>/dev/null; then
		echo "usage: $0 [show|<hostname>] <lock count> ..."
	  exit 0
	fi
done

for lcnt in $*; do
	filename=locks.$lcnt.$host.out
	if $show; then
		[ -f $filename ] || continue
		for fcnt in 1024; do
			for w in 1 10 100 1000 10000 100000; do
				grep -e "f: $fcnt w:.* $w "  $filename |sort -gr -k8
				echo
			done
		done
		continue
	fi
	for t in "${!TYPES[@]}"; do
		PREFIX=$(echo ${TYPES[$t]}|cut -f1 -d:)
		MUTEXNAME=$(echo ${TYPES[$t]}|cut -f2 -d:)
		MUTEXLOCK=$(echo ${TYPES[$t]}|cut -f3 -d:)
		sed -i -e "s/typedef FredMutex shim_mutex_t;/typedef ${MUTEXLOCK} shim_mutex_t; \/\/ test/" apps/include/libfibre.h
		echo -n "========== ${MUTEXNAME} / $lcnt locks ..."
		make clean > compile.out
		make -j $(nproc) -C apps ${PREFIX}threadtest >> compile.out || {
			echo " failed =========="
			continue
		}
		echo " running =========="
		for w in 1 10 100 1000 10000; do
			for f in 1024; do
				[ -f $filename ] && grep -q -e "t:.* $MUTEXNAME f:.* $f w:.* $w " $filename && continue
				# perf stat -e task-clock --log-fd 1 -x,
				taskset -c 32-63 perf stat -e task-clock -o perf.out \
			  ./apps/${PREFIX}threadtest -l$lcnt -t32 -w$w -u$w -f$f | tee run.out
			  thr=$(cat run.out|grep -F loops/fibre|awk '{print $4}')
			  cpu=$(cat perf.out|grep -F "CPUs utilized"|awk '{print $5}')
			  printf "t: %9s f: %4d w: %6d o: %10d u: %6.3f\n" $MUTEXNAME $f $w $thr $cpu >> $filename
			done
		done
	  sed -i -e "s/typedef .* shim_mutex_t; \/\/ test/typedef FredMutex shim_mutex_t;/" apps/include/libfibre.h
		rm -f compile.out perf.out run.out
	done
done
exit 0
