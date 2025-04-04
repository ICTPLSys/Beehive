// This is derived from Jeff Preshing's blog post, available at:
//
// http://preshing.com/20120515/memory-reordering-caught-in-the-act/
//
// Note: The original source code has been published without license terms.
//       It is used here with permission from the author.

#if defined(PTHREADS)
#include <pthread.h>
#include <semaphore.h>
#define fibre_sem_t sem_t
#define fibre_sem_init sem_init
#define fibre_sem_wait sem_wait
#define fibre_sem_post sem_post
#else
#include "fibre.h"
#endif

#include <cassert>
#include <cstdio>

#if defined(__LIBFIBRE__)

#define USE_MUTEX                  0
static FredMutex mutex;

#endif /* __LIBFIBRE__ */

// Set either of these to 1 to prevent CPU reordering
#define USE_CPU_FENCE              0
#define USE_SINGLE_HW_THREAD       0  // Supported on Linux, but not Cygwin or PS3

#if USE_SINGLE_HW_THREAD
#include <sched.h>
#endif


//-------------------------------------
//  MersenneTwister
//  A thread-safe random number generator with good randomness
//  in a small number of instructions. We'll use it to introduce
//  random timing delays.
//-------------------------------------
#define MT_IA  397
#define MT_LEN 624

class MersenneTwister
{
    unsigned int m_buffer[MT_LEN];
    int m_index;

public:
    MersenneTwister(unsigned int seed);
    // Declare noinline so that the function call acts as a compiler barrier:
    unsigned int integer() __attribute__((noinline));
};

MersenneTwister::MersenneTwister(unsigned int seed)
{
    // Initialize by filling with the seed, then iterating
    // the algorithm a bunch of times to shuffle things up.
    for (int i = 0; i < MT_LEN; i++)
        m_buffer[i] = seed;
    m_index = 0;
    for (int i = 0; i < MT_LEN * 100; i++)
        integer();
}

unsigned int MersenneTwister::integer()
{
    // Indices
    int i = m_index;
    int i2 = m_index + 1; if (i2 >= MT_LEN) i2 = 0; // wrap-around
    int j = m_index + MT_IA; if (j >= MT_LEN) j -= MT_LEN; // wrap-around

    // Twist
    unsigned int s = (m_buffer[i] & 0x80000000) | (m_buffer[i2] & 0x7fffffff);
    unsigned int r = m_buffer[j] ^ (s >> 1) ^ ((s & 1) * 0x9908B0DF);
    m_buffer[m_index] = r;
    m_index = i2;

    // Swizzle
    r ^= (r >> 11);
    r ^= (r << 7) & 0x9d2c5680UL;
    r ^= (r << 15) & 0xefc60000UL;
    r ^= (r >> 18);
    return r;
}


//-------------------------------------
//  Main program, as decribed in the post
//-------------------------------------
fibre_sem_t beginSema1;
fibre_sem_t beginSema2;
fibre_sem_t endSema;

int X, Y;
int r1, r2;

void *thread1Func(void*)
{
    MersenneTwister random(1);
    for (;;)
    {
        fibre_sem_wait(&beginSema1);  // Wait for signal
        while (random.integer() % 8 != 0) {}  // Random delay

#if USE_MUTEX
        mutex.acquire();
#endif
        // ----- THE TRANSACTION! -----
        X = 1;
#if USE_CPU_FENCE
        asm volatile("mfence" ::: "memory");  // Prevent CPU reordering
#else
        asm volatile("" ::: "memory");  // Prevent compiler reordering
#endif
        r1 = Y;
#if USE_MUTEX
        mutex.release();
#endif

        fibre_sem_post(&endSema);  // Notify transaction complete
    }
    return NULL;  // Never returns
};

void *thread2Func(void*)
{
    MersenneTwister random(2);
    for (;;)
    {
        fibre_sem_wait(&beginSema2);  // Wait for signal
        while (random.integer() % 8 != 0) {}  // Random delay

#if USE_MUTEX
        mutex.acquire();
#endif
        // ----- THE TRANSACTION! -----
        Y = 1;
#if USE_CPU_FENCE
        asm volatile("mfence" ::: "memory");  // Prevent CPU reordering
#else
        asm volatile("" ::: "memory");  // Prevent compiler reordering
#endif
        r2 = X;
#if USE_MUTEX
        mutex.release();
#endif

        fibre_sem_post(&endSema);  // Notify transaction complete
    }
    return NULL;  // Never returns
};

int main()
{
#if defined(__LIBFIBRE__)
    FibreInit();
#endif

    // Initialize the semaphores
    fibre_sem_init(&beginSema1, 0, 0);
    fibre_sem_init(&beginSema2, 0, 0);
    fibre_sem_init(&endSema, 0, 0);

    // Spawn the threads
#if defined(__LIBFIBRE__)
    Context::CurrCluster().addWorkers(2);
    (new Fibre)->run(thread1Func, (void*)nullptr);
    (new Fibre)->run(thread2Func, (void*)nullptr);
#else
    pthread_t thread1, thread2;
    pthread_create(&thread1, NULL, thread1Func, NULL);
    pthread_create(&thread2, NULL, thread2Func, NULL);
#endif

#if USE_SINGLE_HW_THREAD
    // Force thread affinities to the same cpu core.
    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    CPU_SET(0, &cpus);
    pthread_setaffinity_np(thread1, sizeof(cpu_set_t), &cpus);
    pthread_setaffinity_np(thread2, sizeof(cpu_set_t), &cpus);
#endif

    // Repeat the experiment ad infinitum
    int detected = 0;
    for (int iterations = 1; ; iterations++)
    {
        // Reset X and Y
        X = 0;
        Y = 0;
        // Signal both threads
        fibre_sem_post(&beginSema1);
        fibre_sem_post(&beginSema2);
        // Wait for both threads
        fibre_sem_wait(&endSema);
        fibre_sem_wait(&endSema);
        // Check if there was a simultaneous reorder
        if (r1 == 0 && r2 == 0)
        {
            detected++;
            printf("%d reorders detected after %d iterations\n", detected, iterations);
        }
    }
    return 0;  // Never returns
}
