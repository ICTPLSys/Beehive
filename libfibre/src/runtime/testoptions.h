// **** general options - testing

#define TESTING_ENABLE_ASSERTIONS     0
#define TESTING_ENABLE_STATISTICS     0
#define TESTING_ENABLE_DEBUGGING      0

// **** general options - alternative design

#define TESTING_LOADBALANCING         1 // enable load balancing using ISRS
#define TESTING_GO_IDLEMANAGER        1 // Go/Rust-style idle management
//#define TESTING_DEFAULT_AFFINITY      1 // enable default affinity (no sticky stealing)
//#define TESTING_WAKE_FRED_WORKER      1 // idle manager: wake fred's worker vs any worker
//#define TESTING_LOCKED_READYQUEUE     1 // locked vs. lock-free ready queue
//#define TESTING_STUB_QUEUE            1 // nemesis vs. stub-based MPSC lock-free queue

#include "runtime-glue/testoptions.h"

/******************************** lock options ********************************/

//#define FAST_MUTEX_TYPE SimpleMutex0<false>

//#define FRED_MUTEX_TYPE FastMutex

/******************************** sanity checks ********************************/

#if TESTING_WAKE_FRED_WORKER && !TESTING_LOADBALANCING
  #error TESTING_WAKE_FRED_WORKER requires TESTING_LOADBALANCING
#endif
