/******************************************************************************
    Copyright  2018 Saman Barghi
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"runtime/debug"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// configuration options
var (
	duration    int
	fibreCount  int
	lockCount   int
	threadCount int
	unlocked    int
	work_locked int
	yieldFlag   bool
	serialFlag  bool
	calibration bool
	yieldExperiment bool
)

// other parameters
var (
	// workers goroutines
	workers []Worker
	// locks
	locks []Lock
	// barrier and signal
	barrier   sync.WaitGroup
	startChan chan bool
	// work
	timerOverhead int64
	// manage experiment duration
	ticks   int
	running uint32 // atomic boolean flag
)

type Worker struct {
	counter uint64
}

type Lock struct {
	counter uint64
	lock    sync.Mutex
}

func alarmHandler() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		ticks += 1
		if ticks >= duration {
			fmt.Fprintf(os.Stderr, "\r")
			atomic.StoreUint32(&running, 0)
			break
		} else {
			fmt.Fprintf(os.Stderr, "\r%d", ticks)
		}
	}
}

func opts() {
	flag.IntVar(&duration, "d", 10, "duration of experiment")
	flag.IntVar(&fibreCount, "f", 4, "number of fibres")
	flag.IntVar(&lockCount, "l", 1, "number of locks")
	flag.IntVar(&threadCount, "t", 2, "number of system threads")
	flag.IntVar(&unlocked, "u", 10000, "amount of unlocked work")
	flag.IntVar(&work_locked, "w", 10000, "amount of locked work")
	flag.BoolVar(&yieldFlag, "y", false, "yield flag")
	flag.BoolVar(&serialFlag, "s", false, "serial flag")
	flag.BoolVar(&calibration, "c", false, "run calibration")
	flag.BoolVar(&yieldExperiment, "Y", false, "run yield experiment")
	flag.Parse()
}

const workBufferSize = 16;

func nano(t time.Duration) int64 {
	return time.Duration.Nanoseconds(t) + int64(time.Duration.Seconds(t) * float64(10<<9))
}

func dowork(buffer [workBufferSize]int, steps int) {
  value := 0;
  for i := 0; i < steps; i++ {
    value += (buffer[i % workBufferSize] * 17) / 23 + 55;
  }
  buffer[0] += value;
}

func calibrateTimer() {
	start := time.Now()
	for i := 0; i < (1 << 24) - 1; i += 1 {
		_ = time.Now()
	}
	end := time.Now()
	timerOverhead = nano(end.Sub(start)) / (1 << 24)
}

func calibrateInterval(period int) int {
	// set up work buffer
	var buffer [workBufferSize]int;
	for i := 0; i < workBufferSize; i++ { buffer[i] = rand.Intn(1024) }
	low := 1
	high := 2
	runs := (1<<28) / period;
	fmt.Print(period, "ns - upper bound:")
	for {
		fmt.Print(" ", high);
		start := time.Now()
		for i := 0; i < runs; i++ { dowork(buffer, high) }
		end := time.Now()
		if int((nano(end.Sub(start)) - timerOverhead) / int64(runs)) > period { break; }
		high = high * 2
	}
	fmt.Println()
	fmt.Print("binary search:")
	for {
		fmt.Print(" [", low, ":", high, "]")
		next := (low + high) / 2
		if next == low { break }
		const SampleCount = 3
		var samples []int
		samples = make([]int, SampleCount)
		for s := 0; s < SampleCount; s++ {
			start := time.Now()
			for i := 0; i < runs; i++ { dowork(buffer, next) }
			end := time.Now()
			samples[s] = int((nano(end.Sub(start)) - timerOverhead) / int64(runs))
		}
		sort.Ints(samples)
		if samples[SampleCount/2] > period {
			high = next
		} else {
			low = next
		}
	}
	fmt.Println()
	return high
}

func yielder(num int) {
  // signal creation
  barrier.Done()
  // wait for start signal
  <-startChan
  var count uint64 = 0
  for atomic.LoadUint32(&running) != 0 {
	  runtime.Gosched()
	  count += 1
	}
	workers[num].counter = count
	barrier.Done()
}

func worker(num int) {
	// set up work buffer
	var buffer [workBufferSize]int;
	for i := 0; i < workBufferSize; i++ { buffer[i] = rand.Intn(1024) }

	// initialize
	lck := rand.Intn(lockCount)

	// signal creation
	barrier.Done()
	// wait for start signal
	<-startChan

	for atomic.LoadUint32(&running) != 0 {
		// unlocked work
		dowork(buffer, unlocked)

		// locked work and counters
		locks[lck].lock.Lock()
		dowork(buffer, work_locked)
		workers[num].counter += 1
		locks[lck].counter += 1
		locks[lck].lock.Unlock()

		if yieldFlag {
			runtime.Gosched()
		}

		// pick next lock, serial or random
		if serialFlag {
			lck = (lck + 1) % lockCount
		} else {
			lck = rand.Intn(lockCount)
		}
	} // for
	barrier.Done()
}

func main() {
	// parse command line arguments
	opts()

	if (duration == 0 || fibreCount == 0 || lockCount == 0 || threadCount == 0) {
		fmt.Println("none of -d, -f, -l, -t can be zero");
		os.Exit(1)
	}

	// Set number of threads
	runtime.GOMAXPROCS(threadCount)

	// turn off garbage collection
	debug.SetGCPercent(-1)

	// set up random number generator
	rand.Seed(time.Now().UTC().UnixNano())

	if calibration {
		calibrateTimer()
		fmt.Println("overhead:", timerOverhead)
		w := calibrateInterval(work_locked)
		fmt.Println("WORK: -w", w)
		u := calibrateInterval(unlocked)
		fmt.Println("UNLOCKED work: -u", u)
		fmt.Println()
		fmt.Print("WARNING: these numbers might not necessarily be accurate.")
		fmt.Println(" Double-check the actual runtime with 'perf'");
		fmt.Println()
		os.Exit(0)
	}

	// create thread and lock structures
	workers = make([]Worker, fibreCount)
	locks = make([]Lock, lockCount)
	startChan = make(chan bool)

	// set up start barrier
	barrier.Add(fibreCount)

	// create gorotuines
	for index, _ := range workers {
		if yieldExperiment {
			go yielder(index)
		} else {
			go worker(index)
		}
	}

	// running = true
	atomic.StoreUint32(&running, 1)

	// start the timer
	go alarmHandler()

	// wait for goroutine creation
	barrier.Wait()

	// signal the beginning of the experiment
	startTime := time.Now()
	close(startChan)

	// wait for all workers to finish
	barrier.Add(fibreCount)
	barrier.Wait()
	endTime := time.Now()

	// print configuration
	fmt.Print("threads: ", threadCount, " workers: ", fibreCount, " locks: ", lockCount)
	if serialFlag { fmt.Print(" serial") }
	if yieldFlag { fmt.Print(" yield") }
	fmt.Println()
	fmt.Println("duration:", duration, "work:", work_locked, "unlocked work:", unlocked)

	// collect and print work results
	var wsum uint64
	var wsum2 float64

	for _, element := range workers {
		wsum += element.counter
		wsum2 += math.Pow(float64(element.counter), 2)
	}

	wavg := float64(wsum) / float64(fibreCount)
	wstd := math.Sqrt(float64(wsum2)/float64(fibreCount) - math.Pow(wavg, 2))

	fmt.Print("work - total: ", wsum, " rate: ", int64(float64(wsum)/float64(duration)), " fairness: ", int64(wavg), "/", int64(wstd))
	fmt.Println()

	// collect and print lock results
	var lsum uint64
	var lsum2 float64

	for _, element := range locks {
		lsum += element.counter
		lsum2 += math.Pow(float64(element.counter), 2)
	}

	lavg := float64(lsum) / float64(lockCount)
	lstd := math.Sqrt(float64(lsum2)/float64(lockCount) - math.Pow(lavg, 2))

	fmt.Print("lock - total: ", lsum, " rate: ", int64(float64(lsum)/float64(duration)), " fairness: ", int64(lavg), "/", int64(lstd))
	fmt.Println()

	if yieldExperiment {
		fmt.Print("time spent (nanoseconds): ", nano(endTime.Sub(startTime)))
		fmt.Println()
		fmt.Print("time per yield: ", nano(endTime.Sub(startTime)) / int64(wsum / uint64(threadCount)))
		fmt.Println()
	}

	// clean up is handled by go
}
