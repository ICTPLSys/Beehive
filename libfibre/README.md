Fred / Libfibre
===============

Fred / Libfibre is an M:N user-level threading runtime without preemption, thus the term <i>fibre</i>. It demonstrably supports massive session concurrency in network/cloud servers with minimal overhead.

Running `make all` builds the fibre library in `src/libfibre.so` along with several example/test programs: `test1`, `ordering`, `threadtest`, `echotest`, and `webserver` in the subdirectory `apps`.

The build process should download all git submodules.
If that fails, download manually using `git submodule update --init --recursive`.

Running `make doc` builds documentation in `doc/html/index.html`.

Both Linux/epoll and FreeBSD/kqueue are supported, but significantly more testing has been done for Linux/epoll.

### Results

The runtime system has been described and evaluated in our Sigmetrics 2020
paper [User-level Threading: Have Your Cake and Eat It Too](https://cs.uwaterloo.ca/~mkarsten/papers/sigmetrics2020.html).
In this paper, it has been named <b>Fred</b> to satsify anonymity
requirements during peer review - and to distinguish it from other user-level threading runtimes.

### Contributors

The runtime has originally been developed in close collaboration with Saman Barghi.

In addition, the following students (in alphabetical order) have helped with various parts of Libfibre:

- Qin An (FreeBSD/kqueue)
- Bilal Akhtar (gdb extension)
- Peter Cai (idle manager, bug fixes)
- Zhaocheng Che (on-demand oneshot polling)
- Peng Chen (split stack)
- Bryant Curto (bug fixes)
- Peiran Hong (API/Apache)
- Matthew May (MCS queue with timeout)
- Vrajang Parikh (gdb names & watchpoints)
- Wen Shi (gdb extension)
- Kai Sun (ARM64, mutex locks)
- JinYang (Luke) Tao (LTTng)
- Shuangyi Tong (event scopes)
- Gan Wang (API/Apache)
- Xinyi Zhou (worker/core affinity)
- Zhenyan Zhu (std::thread using fibre)

 All bugs are mine though. ;-)

### License

Libfibre is currently distributed under the GNU GPL license, although this could change in the future. See <tt>[LICENSE](LICENSE)</tt> for details.

### Feedback / Questions

Please send any questions or feedback to mkarsten|**at**|uwaterloo.ca.
