// **** libfibre options - event handling

//#define TESTING_WORKER_POLLER         1 // poll events during idle loop
//#define TESTING_WORKER_IO_URING       1 // process io_uring events during idle loop (Linux only)

#define TESTING_CLUSTER_POLLER_FIBRE  1 // per-cluster poller(s): fibre vs. pthread
#define TESTING_EVENTPOLL_TRYREAD     1 // try nonblocking input operation first
//#define TESTING_EVENTPOLL_EDGE        1 // use edge-trigger event polling
#define TESTING_EVENTPOLL_ONESHOT     1 // use oneshot event polling
//#define TESTING_EVENTPOLL_ONDEMAND    1 // use ondemand event polling
//#define TESTING_POLLER_FIBRE_SPIN 65536 // poller fibre: spin loop of NB polls

//#define TESTING_IO_URING_DEFAULT      1 // make io_uring default for sockets

/******************************** lock options ********************************/

//#define TESTING_LOCK_RECURSION        1 // enable mutex recursion in C interface

//#define WORKER_LOCK_TYPE BinaryLock<>

/******************************** sanity checks ********************************/

#if TESTING_CLUSTER_POLLER_FIBRE && !TESTING_LOADBALANCING
  #error TESTING_CLUSTER_POLLER_FIBRE requires TESTING_LOADBALANCING
#endif

#if TESTING_EVENTPOLL_EDGE && !TESTING_EVENTPOLL_TRYREAD
  #error edge-triggered polling requires TESTING_EVENTPOLL_TRYREAD
#endif

#if TESTING_WORKER_IO_URING
 #if !__linux__
  #error TESTING_WORKER_IO_URING is only available on Linux
 #endif
 #if TESTING_WORKER_POLLER
  #error TESTING_WORKER_IO_URING and TESTING_WORKER_POLLER cannot be combined
 #endif
#else
 #if TESTING_IO_URING_DEFAULT
  #error TESTING_IO_URING_DEFAULT requires TESTING_WORKER_IO_URING
 #endif
#endif
