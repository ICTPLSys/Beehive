#!/usr/bin/env bash

function error() {
  echo " ERROR " $1
  exit 1
}

function check() {
	if [ "$(uname -s)" = "FreeBSD" ]; then
		[ $(sysctl kern.ipc.somaxconn|awk '{print $2}') -lt 4096 ] && error "kern.ipc.somaxconn should be at least 4096"
		[ $(ulimit -n) -lt 65536 ] && error "ulimit -n should be at least 65536"
		maxconn=100 # there's some kind of new limit in FreeBSD 13.0?
	else
		[ $(sysctl net.core.somaxconn|awk '{print $3}') -lt 4096 ] && error "net.core.somaxconn should be at least 4096"
		[ $(ulimit -n) -lt 65536 ] && error "ulimit -n should be at least 65536"
		[ $(sysctl kernel.perf_event_paranoid|awk '{print $3}') -lt 1 ] || error "kernel.perf_event_paranoid should be less than 1"
		maxconn=500
	fi
}

function init() {
	mkdir -p out
	[ -f /usr/local/lib/liburing.so ] && LOCALURING=/usr/local/lib
	if [ "$(uname -s)" = "FreeBSD" ]; then
		MAKE=gmake
		count=$(expr $(sysctl kern.smp.cpus|cut -c16- || echo 1) / 4)
		[ $count -gt 0 ] || count=1
		$HT && clcnt=$(expr $count \* 2) || clcnt=$(expr $count \* 3)
		clbot=0
		cltop=$(expr $clcnt - 1)
		svtop=$(expr $count \* 4 - 1)
		svlist=$clcnt
		$HT && inc=2 || inc=1
		for ((c=$svlist+$inc;c<=$svtop;c+=$inc)); do
			svlist=$svlist,$c
		done
		$quiet || echo CORES: client $clbot-$cltop server $svlist
		TASKSET_SERVER="cpuset -l $svlist"
		TASKSET_CLIENT="cpuset -l $clbot-$cltop"
		TEST_MEMCACHED_PORT="sockstat -46l -p 11211"
	else
		MAKE=make
		count=$(expr $(nproc) / 4)
		[ $count -gt 0 ] || count=1
		clbot1=0
		cltop1=$(expr $count - 1)
		clbot2=$(expr $count \* 2)
		cltop2=$(expr $count \* 3 - 1)
		svbot=$(expr $count \* 3)
		svtop=$(expr $count \* 4 - 1)
		$HT && clcnt=$(expr $count \* 2) || clcnt=$(expr $count \* 3)
		$HT && cllst="$clbot1-$cltop1,$clbot2-$cltop2" || cllst="$clbot1-$cltop2"
		$quiet || echo CORES: client $cllst server $svbot-$svtop
		TASKSET_SERVER="taskset -c $svbot-$svtop"
		TASKSET_CLIENT="taskset -c $cllst"
		TEST_MEMCACHED_PORT="lsof -i :11211"
	fi
}

function clean() {
	$quiet || echo -n "CLEAN"
  ($MAKE clean > out/clean.$1.out; $MAKE -C memcached-1.6.9 clean distclean; $MAKE -C vanilla clean distclean) > out/clean.$1.out 2>> out/clean.$1.err
}

function compile() {
	$MAKE diff > out/diff.$2.out
  $quiet || echo -n " COMPILE"
  case "$1" in
  stacktest)
		$MAKE DYNSTACK=1 all > out/make.$2.out || error
		;;
  memcached)
	  $MAKE all > out/make.$2.out || error
		FPATH=$PWD
		(cd memcached; ./configure2.sh $FPATH $LOCALURING; cd -) >> out/make.$2.out || error
		$MAKE -C memcached -j $count all >> out/make.$2.out || error
		;;
	vanilla)
		(cd vanilla; ./configure; cd -) > out/make.$2.out || error
		$MAKE -C vanilla -j $count all >> out/make.$2.out || error
		;;
	skip)
		;;
	*)
	  $MAKE all > out/make.$2.out || error
	  ;;
	esac
	$quiet || echo " DONE"
}

function post() {
	killall -9 threadtest > /dev/null 2> /dev/null
	killall -9 stacktest > /dev/null 2> /dev/null
	killall -9 memcached > /dev/null 2> /dev/null
	$MAKE -C memcached clean distclean > /dev/null 2> /dev/null
	$MAKE -C vanilla clean distclean > /dev/null 2> /dev/null
	$MAKE clean > /dev/null 2> /dev/null
}

function run_skip() {
	echo "test skipped"
}

function run_threadtest() {
	c=$(expr $count \* 2)
	CMD="FibrePrintStats=1 timeout 30 ./apps/threadtest -t $c -f $(expr $c \* $c)"
	$show && echo $CMD || eval $CMD || error
	CMD="FibrePrintStats=1 timeout 30 ./apps/threadtest -t $c -f $(expr $c \* $c) -o 10000 -r -L T"
	$show && echo $CMD || eval $CMD || error
}

function run_stacktest() {
	$show || echo -n "STACKTEST: "
	CMD="FibrePrintStats=1 timeout 30 apps/stacktest > out/stacktest.$1.out"
	$show && echo $CMD || eval $CMD && echo SUCCESS || echo FAILURE
}

function run_memcached_one() {
	for ((p=0;;p++)); do
		$TEST_MEMCACHED_PORT | grep -F -q 11211 > /dev/null || break
		[ $p -ge 3 ] && error "memcached port not free"
		sleep 1
	done

  CMD="FibrePollerCount=$pcount FibreStatsSignal=0 FibrePrintStats=t $TASKSET_SERVER $prog \
  -t $count -b 32768 -c 32768 -m 10240 -o hashpower=24"
  $show && echo $CMD || (eval $CMD > out/memcached.$exp.$cnt.out & sleep 1)

	q=$1
	shift
  CMD="timeout 10 mutilate -s0 --loadonly -r 100000 -K fb_key -V fb_value"
  $show && echo $CMD || eval $CMD || error
  $show || printf "RUN: %4s %5s" $*

	[ "$(uname -s)" = "FreeBSD" ] && {
		RUN="$TASKSET_CLIENT" # pmcstat -P uops_retired.total_cycles -t '^memcached$'
	} || {
		RUN="$TASKSET_CLIENT perf stat -d --no-big-num -p \$(pidof memcached) -o out/perf.$exp.$cnt.out"
	}

  CMD="$RUN timeout 30 \
  mutilate -s0 --noload -r 100000 -K fb_key -V fb_value -i fb_ia -t10 -u$update -T$clcnt -q$q $*"
  $show && echo $CMD || (eval $CMD > out/mutilate.$exp.$cnt.out || error)

	$show && return

  (echo stats;sleep 1)|telnet localhost 11211 \
  2> >(grep -F -v "Connection closed" 1>&2) > out/stats.$exp.$cnt.out

  killall memcached && sleep 1 || error
}

function output_memcached() {
  lline=$(grep -F read out/mutilate.$exp.$cnt.out)
  lat=$(echo $lline|awk '{print $2}'|cut -f1 -d.)
  lvr=$(echo $lline|awk '{print $3}'|cut -f1 -d.)
  rline=$(grep -F rx out/mutilate.$exp.$cnt.out)
  [ -z "$rline" ] && {
		rat=0
		rvr=0
		cov=999
  } || {
		rat=$(echo $rline|awk '{print $2}'|cut -f1 -d.)
		rvr=$(echo $rline|awk '{print $3}'|cut -f1 -d.)
		cov=$(expr $rvr \* 100 / $rat)
  }
  qline=$(grep -F "QPS" out/mutilate.$exp.$cnt.out)
  qps=$(echo $qline|awk '{print $4}'|cut -f1 -d.)
  req=$(echo $qline|cut -f2 -d'('|awk '{print $1}')
	buf=$(grep -F read_buf_count out/stats.$exp.$cnt.out|awk '{print $3}')
	jug=$(grep -F lru_maintainer_juggles out/stats.$exp.$cnt.out|awk '{print $3}')

	$exact && {
		printf " QPS: %7d RAT: %7d %7d LAT: %7d %7d" \
		$qps $rat $rvr $lat $lvr
	} || {
		printf " QPS: %4dK COV: %3d%% LAT: %4du %4du" \
		$(expr $qps / 1000) $cov $(expr $lat / 1000) $(expr $lvr / 1000)
	}

	[ "$(uname -s)" = "FreeBSD" ] || {
		cyc=$(grep -F "cycles" out/perf.$exp.$cnt.out|grep -F -v stalled|awk '{print $1}')
		ins=$(grep -F "instructions" out/perf.$exp.$cnt.out|awk '{print $1}')
		l1c=$(grep -F "L1-dcache-load-misses" out/perf.$exp.$cnt.out|awk '{print $1}')
		llc=$(grep -F "LLC-load-misses" out/perf.$exp.$cnt.out|awk '{print $1}')
		cpu=$(grep -F "CPUs utilized" out/perf.$exp.$cnt.out|awk '{print $5}')

		[ "$l1c" = "<not" ] && l1c=0
		[ "$llc" = "<not" ] && llc=0

	  $exact && {
		  printf " L1C: %4d LLC: %4d INS: %6d CYC %6d CPU %7.3f" \
		  $(expr $l1c / $req) $(expr $llc / $req) $(expr $ins / $req) $(expr $cyc / $req) $cpu
	  } || {
		  printf " L1C: %4d LLC: %4d INS: %3dK CYC %3dK CPU %7.3f" \
		  $(expr $l1c / $req) $(expr $llc / $req) $(expr $ins / $req / 1000) $(expr $cyc / $req / 1000) $cpu
	  }
  }

	printf " buf: %6d" $buf

  $exact && {
	  printf " jug: %6d\n" $jug
  } || {
	  printf " jug: %3dK\n" $(expr $jug / 1000)
	}
}

function run_memcached_all() {
	exp=$1
	shift
	cnt=1
	for d in 01 64; do
		for c in 025 $maxconn; do
			[ $# -gt 0 ] && { q=$1; shift; } || q=0
			run_memcached_one $q -d$d -c$c
			$show || output_memcached
			cnt=$(expr $cnt + 1)
		done
	done
}

function run_memcached() {
	prog=memcached/memcached
	run_memcached_all $*
}

function run_vanilla() {
	prog=vanilla/memcached
	run_memcached_all $*
}

function prep_value() {
	$MAKE gen
	while [ $# -gt 0 ]; do
		file=$(grep -F -l "define $2" src/runtime*/testoptions.h)
		[ -f $file ] || error
		case $1 in
			0) sed -i -e "s/.*#define $2 .*/#undef $2/" $file;;
			*) sed -i -e "s/.*#define $2 .*/#define $2 $1/" $file;;
		esac
		shift 2
	done
}

function prep_0() {
	app=vanilla
}

function prep_1() {
	app=threadtest
}

function prep_2() {
	[ "$(uname -s)" = "FreeBSD" ] && app=skip && return
	[ "$(uname -m)" = "aarch64" ] && app=skip && return
	app=stacktest
}

function prep_3() {
	app=memcached
}

function prep_4() {
	prep_value 0 TESTING_CLUSTER_POLLER_FLOAT\
	           1 TESTING_EVENTPOLL_EDGE\
	           0 TESTING_EVENTPOLL_ONESHOT
	app=memcached
}

function prep_5() {
	prep_value 1 TESTING_WORKER_POLLER\
	           0 TESTING_CLUSTER_POLLER_FLOAT\
	           0 TESTING_EVENTPOLL_TRYREAD\
	           0 TESTING_EVENTPOLL_ONESHOT
	app=memcached
}

function prep_6() {
	prep_value 0 TESTING_LOADBALANCING\
	           0 TESTING_CLUSTER_POLLER_FIBRE
	app=memcached
}

function prep_7() {
	prep_value 0 TESTING_DEFAULT_AFFINITY
	app=memcached
}

function prep_8() {
	prep_value 1 TESTING_STUB_QUEUE
	app=memcached
}

function prep_9() {
	[ "$(uname -s)" = "FreeBSD" ] && app=skip && return
	[ ! -f /usr/include/liburing.h ] && [ ! -f /usr/local/include/liburing.h ] && app=skip && return
	prep_value 1 TESTING_WORKER_IO_URING
	app=memcached
}

function prep_10() {
	[ "$(uname -s)" = "FreeBSD" ] && app=skip && return
	[ ! -f /usr/include/liburing.h ] && [ ! -f /usr/local/include/liburing.h ] && app=skip && return
  prep_value 1 TESTING_WORKER_IO_URING\
             1 TESTING_IO_URING_DEFAULT
	app=memcached
}

function runexp() {
	header=$(
		echo -n "EXPERIMENT $*: HT=$HT"
		$exact && echo -n " -e"
		$quiet && echo -n " -q"
		echo " -p$pcount -u$update"
	)
	$quiet || $show || echo -n "$header "
	echo $header > out/config.$1.out
	$show || clean $1
  prep_$1
  $show || compile $app $1
  run_$app $*
  post
}

# "main routine" starts here

check

[ -z "$HT" ] && HT=true

emax=10

exact=false
pcount=0
quiet=false
show=false
update=0.1

while getopts "ep:qsu:" option; do
  case $option in
	e) exact=true;;
	p) pcount="${OPTARG}";;
	q) quiet=true;;
	s) show=true;;
	u) update="${OPTARG}";;
	esac
done
shift $(($OPTIND - 1))

init

[ $pcount -lt 1 ] && pcount=$(expr $count / 4)
[ $pcount -lt 1 ] && pcount=1

if [ $# -gt 0 ]; then
	if [ $1 -lt 0 -o $1 -gt $emax ]; then
		echo argument out of range
		exit 1
	fi
	trap post EXIT
	runexp $*
	exit 0
fi

if [ "$(uname -s)" = "Linux" ]; then
	$MAKE CC=clang all || error
	$MAKE CC=clang clean || error
fi

for ((e=1;e<=$emax;e+=1)); do
	trap "killall -9 memcached > /dev/null 2> /dev/null" EXIT
	runexp $e $*
done

exit 0
