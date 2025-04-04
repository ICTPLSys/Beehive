/******************************************************************************
    Copyright (C) Martin Karsten 2015-2023

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
#ifndef _cfibre_h_
#define _cfibre_h_ 1

#ifdef __GNUC__
#define restrict __restrict__
#else
#define restrict
#endif

/**
 @file
 Additionally, all routines in fibre.h are available as corresponding 'cfibre' version.
 */

#include <stdlib.h>       // abort()
#include <time.h>         // struct timespec
#include <unistd.h>       // read, write, useconds_t
#include <sys/types.h>    // socket types (FreeBSD)
#include <sys/socket.h>   // socket interface
#if defined(__linux__)
#include <sys/epoll.h>    // epoll (Linux)
#include <sys/sendfile.h> // sendfile (Linux)
#endif
#include <sys/uio.h>      // readv, writev
#include <pthread.h>      // pthread_t

#ifndef __LIBFIBRE__
#define __LIBFIBRE__ 1
#endif

typedef struct _cfibre_t*         cfibre_t;
typedef struct _cfibre_sem_t*     cfibre_sem_t;
typedef struct _cfibre_mutex_t*   cfibre_mutex_t;
typedef struct _cfibre_cond_t*    cfibre_cond_t;
typedef struct _cfibre_rwlock_t*  cfibre_rwlock_t;
typedef struct _cfibre_barrier_t* cfibre_barrier_t;

typedef struct _cfibre_attr_t*        cfibre_attr_t;
typedef struct _cfibre_mutexattr_t*   cfibre_mutexattr_t;
typedef struct _cfibre_condattr_t*    cfibre_condattr_t;
typedef struct _cfibre_rwlockattr_t*  cfibre_rwlockattr_t;
typedef struct _cfibre_barrierattr_t* cfibre_barrierattr_t;

typedef struct _cfibre_fastmutex_t*     cfibre_fastmutex_t;
typedef struct _cfibre_fastmutexattr_t* cfibre_fastmutexattr_t;

typedef struct _cfibre_cluster_t*    cfibre_cluster_t;
typedef struct _cfibre_eventscope_t* cfibre_eventscope_t;

typedef size_t cfibre_key_t;

typedef pthread_once_t cfibre_once_t;
static const cfibre_once_t CFIBRE_ONCE_INIT = PTHREAD_ONCE_INIT;

static inline int cfibre_once(cfibre_once_t *once_control, void (*init_routine)(void)) {
  return pthread_once(once_control, init_routine);
}

static inline int cfibre_cancel(cfibre_t x) {
  (void)x;
  abort();
}

static const int CFIBRE_MUTEX_RECURSIVE  = PTHREAD_MUTEX_RECURSIVE;
static const int CFIBRE_MUTEX_ERRORCHECK = PTHREAD_MUTEX_ERRORCHECK;
static const int CFIBRE_MUTEX_DEFAULT    = PTHREAD_MUTEX_DEFAULT;

#ifdef __cplusplus
extern "C" {
#endif

/** @brief Bootstrap routine should be called early in main(). */
void cfibre_init(void);
/** @brief Alternative bootstrap routine. */
void cfibre_init_n(size_t pollerCount, size_t workerCount);

/** @brief Fork process (with restrictions) and re-initialize runtime in child process (`fork`). */
pid_t cfibre_fork(void);

/** @brief Create Cluster */
int cfibre_cluster_create(cfibre_cluster_t* cluster);
/** @brief Destroy Cluster */
int cfibre_cluster_destroy(cfibre_cluster_t* cluster);
/** @brief Obtain Cluster ID */
cfibre_cluster_t cfibre_cluster_self(void);

/** @brief Add one worker to cluster; execute init_routine first */
int cfibre_add_worker(cfibre_cluster_t cluster, pthread_t* tid, void (*init_routine) (void *), void *arg);

/** @brief Pause (all but current) processors in specified Cluster. */
int cfibre_pause(cfibre_cluster_t cluster);
/** @brief Resume processors in specified Cluster. */
int cfibre_resume(cfibre_cluster_t cluster);

/** @brief Create new event scope. */
cfibre_eventscope_t cfibre_eventscope_clone(void (*mainFunc) (void *), void* mainArg);
/** @brief Obtain current event scope. */
cfibre_eventscope_t cfibre_eventscope_self(void);

/** @brief Read OS-level `errno` variable (special routine due to TLS). */
int cfibre_get_errno(void);
/** @brief Write OS-level `errno` variable (special routine due to TLS). */
void cfibre_set_errno(int);

int cfibre_attr_init(cfibre_attr_t *attr);
int cfibre_attr_destroy(cfibre_attr_t *attr);
int cfibre_attr_setcluster(cfibre_attr_t *attr, cfibre_cluster_t cluster);
int cfibre_attr_getcluster(const cfibre_attr_t *attr, cfibre_cluster_t *cluster);
int cfibre_attr_setstacksize(cfibre_attr_t *attr, size_t stacksize);
int cfibre_attr_getstacksize(const cfibre_attr_t *attr, size_t *stacksize);
int cfibre_attr_setguardsize(cfibre_attr_t *attr, size_t guardsize);
int cfibre_attr_getguardsize(const cfibre_attr_t *attr, size_t *guardsize);
int cfibre_attr_setpriority(cfibre_attr_t *attr, int priority);
int cfibre_attr_getpriority(const cfibre_attr_t *attr, int *priority);
int cfibre_attr_setaffinity(cfibre_attr_t *attr, int affinity);
int cfibre_attr_getaffinity(const cfibre_attr_t *attr, int *affinity);
int cfibre_attr_setdetachstate(cfibre_attr_t *attr, int detachstate);
int cfibre_attr_getdetachstate(const cfibre_attr_t *attr, int *detachstate);

int cfibre_create(cfibre_t *thread, const cfibre_attr_t *attr, void *(*start_routine) (void *), void *arg);
int cfibre_join(cfibre_t thread, void **retval);
int cfibre_detach(cfibre_t thread);
void cfibre_exit(void) __attribute__((__noreturn__));
cfibre_t cfibre_self(void);
int cfibre_equal(cfibre_t thread1, cfibre_t thread2);
int cfibre_yield(void);
int cfibre_key_create(cfibre_key_t *key, void (*destructor)(void*));
int cfibre_key_delete(cfibre_key_t key);
int cfibre_setspecific(cfibre_key_t key, const void *value);
void *cfibre_getspecific(cfibre_key_t key);
int cfibre_park(void);
int cfibre_unpark(cfibre_t thread);
int cfibre_migrate(cfibre_cluster_t cluster);

int cfibre_sem_init(cfibre_sem_t *sem, int pshared, unsigned int value);
int cfibre_sem_destroy(cfibre_sem_t *sem);
int cfibre_sem_wait(cfibre_sem_t *sem);
int cfibre_sem_trywait(cfibre_sem_t *sem);
int cfibre_sem_timedwait(cfibre_sem_t *sem, const struct timespec *abs_timeout);
int cfibre_sem_post(cfibre_sem_t *sem);
int cfibre_sem_getvalue(cfibre_sem_t *sem, int *sval);

int cfibre_mutexattr_init(cfibre_mutexattr_t *attr);
int cfibre_mutexattr_destroy(cfibre_mutexattr_t *attr);
int cfibre_mutexattr_settype(cfibre_mutexattr_t *attr, int type);

int cfibre_mutex_init(cfibre_mutex_t *restrict mutex, const cfibre_mutexattr_t *restrict attr);
int cfibre_mutex_destroy(cfibre_mutex_t *mutex);
int cfibre_mutex_lock(cfibre_mutex_t *mutex);
int cfibre_mutex_trylock(cfibre_mutex_t *mutex);
int cfibre_mutex_timedlock(cfibre_mutex_t *restrict mutex, const struct timespec *restrict abstime);
int cfibre_mutex_unlock(cfibre_mutex_t *mutex);

int cfibre_condattr_init(cfibre_condattr_t *attr);
int cfibre_condattr_destroy(cfibre_condattr_t *attr);

int cfibre_cond_init(cfibre_cond_t *restrict cond, const cfibre_condattr_t *restrict attr);
int cfibre_cond_destroy(cfibre_cond_t *cond);
int cfibre_cond_wait(cfibre_cond_t *restrict cond, cfibre_mutex_t *restrict mutex);
int cfibre_cond_timedwait(cfibre_cond_t *restrict cond, cfibre_mutex_t *restrict mutex, const struct timespec *restrict abstime);
int cfibre_cond_signal(cfibre_cond_t *cond);
int cfibre_cond_broadcast(cfibre_cond_t *cond);

int cfibre_rwlock_init(cfibre_rwlock_t *restrict rwlock, const cfibre_rwlockattr_t *restrict attr);
int cfibre_rwlock_destroy(cfibre_rwlock_t *rwlock);
int cfibre_rwlock_rdlock(cfibre_rwlock_t *rwlock);
int cfibre_rwlock_tryrdlock(cfibre_rwlock_t *rwlock);
int cfibre_rwlock_timedrdlock(cfibre_rwlock_t *restrict rwlock, const struct timespec *restrict abstime);
int cfibre_rwlock_wrlock(cfibre_rwlock_t *rwlock);
int cfibre_rwlock_trywrlock(cfibre_rwlock_t *rwlock);
int cfibre_rwlock_timedwrlock(cfibre_rwlock_t *restrict rwlock, const struct timespec *restrict abstime);
int cfibre_rwlock_unlock(cfibre_rwlock_t *rwlock);

int cfibre_barrier_init(cfibre_barrier_t *restrict barrier, const cfibre_barrierattr_t *restrict attr, unsigned count);
int cfibre_barrier_destroy(cfibre_barrier_t *barrier);
int cfibre_barrier_wait(cfibre_barrier_t *barrier);

int cfibre_fastmutexattr_init(cfibre_fastmutexattr_t *attr);
int cfibre_fastmutexattr_destroy(cfibre_fastmutexattr_t *attr);
int cfibre_fastmutexattr_settype(cfibre_fastmutexattr_t *attr, int type);

int cfibre_fastmutex_init(cfibre_fastmutex_t *restrict mutex, const cfibre_fastmutexattr_t *restrict attr);
int cfibre_fastmutex_destroy(cfibre_fastmutex_t *mutex);
int cfibre_fastmutex_lock(cfibre_fastmutex_t *mutex);
int cfibre_fastmutex_trylock(cfibre_fastmutex_t *mutex);
int cfibre_fastmutex_unlock(cfibre_fastmutex_t *mutex);

int cfibre_fastcond_wait(cfibre_cond_t *restrict cond, cfibre_fastmutex_t *restrict mutex);
int cfibre_fastcond_timedwait(cfibre_cond_t *restrict cond, cfibre_fastmutex_t *restrict mutex, const struct timespec *restrict abstime);

#if defined(__linux__)
int cfibre_epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout);
#endif
/** @brief Create socket. (`socket`). */
int cfibre_socket(int domain, int type, int protocol);
/** @brief Bind socket. (`bind`). */
int cfibre_bind(int socket, const struct sockaddr *address, socklen_t address_len);
/** @brief Set up socket listen queue. (`listen`). */
int cfibre_listen(int socket, int backlog);
/** @brief Accept new connection. (`accept`). */
int cfibre_accept(int socket, struct sockaddr *restrict address, socklen_t *restrict address_len);
/** @brief Accept new connection with flags. (`accept4`). */
int cfibre_accept4(int socket, struct sockaddr *restrict address, socklen_t *restrict address_len, int flags);
/** @brief Create new connection. (`connect`). */
int cfibre_connect(int socket, const struct sockaddr *address, socklen_t address_len);
/** @brief Clone file descriptor. (`dup`). */
int cfibre_dup(int fildes);
/** @brief Create pipe. (`pipe`). */
int cfibre_pipe(int pipefd[2]);
/** @brief Create pipe. (`pipe2`). */
int cfibre_pipe2(int pipefd[2], int flags);
/** @brief Close file descriptor. (`close`). */
int cfibre_close(int fildes);
/** @brief Output via socket. (`send`). */
ssize_t cfibre_send(int socket, const void *buffer, size_t length, int flags);
/** @brief Output via socket. (`sendto`). */
ssize_t cfibre_sendto(int socket, const void *message, size_t length, int flags, const struct sockaddr *dest_addr, socklen_t dest_len);
/** @brief Output via socket. (`sendmsg`). */
ssize_t cfibre_sendmsg(int socket, const struct msghdr *message, int flags);
/** @brief Output via socket/file. (`write`). */
ssize_t cfibre_write(int fildes, const void *buf, size_t nbyte);
/** @brief Output via socket/file. (`writev`). */
ssize_t cfibre_writev(int fildes, const struct iovec *iov, int iovcnt);
/** @brief Receive via socket. (`recv`). */
ssize_t cfibre_recv(int socket, void *buffer, size_t length, int flags);
/** @brief Receive via socket. (`recvfrom`). */
ssize_t cfibre_recvfrom(int socket, void *restrict buffer, size_t length, int flags, struct sockaddr *restrict address, socklen_t *restrict address_len);
/** @brief Receive via socket. (`recvmsg`). */
ssize_t cfibre_recvmsg(int socket, struct msghdr *message, int flags);
/** @brief Receive via socket/file. (`read`). */
ssize_t cfibre_read(int fildes, void *buf, size_t nbyte);
/** @brief Receive via socket/file. (`readv`). */
ssize_t cfibre_readv(int fildes, const struct iovec *iov, int iovcnt);

/** @brief Transmit file via socket. (`sendfile`). */
#if defined(__FreeBSD__)
int cfibre_sendfile(int fd, int s, off_t offset, size_t nbytes, struct sf_hdtr *hdtr, off_t *sbytes, int flags);
#else
ssize_t cfibre_sendfile(int out_fd, int in_fd, off_t *offset, size_t count);
#endif

/** @brief Set socket flags (`fcntl`). */
int cfibre_fcntl(int fildes, int cmd, int flags);

/** @brief Sleep fibre. (`nanosleep`). */
int cfibre_nanosleep(const struct timespec *rqtp, struct timespec *rmtp);
/** @brief Sleep fibre. (`usleep`). */
int cfibre_usleep(useconds_t uses);
/** @brief Sleep fibre. (`sleep`). */
int cfibre_sleep(unsigned int secs);

#ifdef __cplusplus
}
#endif

#endif /* _cfibre_h_ */
