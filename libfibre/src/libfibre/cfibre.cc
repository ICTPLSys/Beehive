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
#include "libfibre/fibre.h"
#include "libfibre/cfibre.h"

#include <cassert>
#include <sys/uio.h>      // readv, writev
#if !defined(__FreeBSD__)
#include <sys/sendfile.h> // sendfile
#endif

struct _cfibre_t         : public Fibre {};
struct _cfibre_sem_t     : public fibre_sem_t {};
struct _cfibre_mutex_t   : public fibre_mutex_t {};
struct _cfibre_cond_t    : public fibre_cond_t {};
struct _cfibre_rwlock_t  : public fibre_rwlock_t {};
struct _cfibre_barrier_t : public fibre_barrier_t {};

struct _cfibre_attr_t        : public fibre_attr_t {};
struct _cfibre_mutexattr_t   : public fibre_mutexattr_t {};
struct _cfibre_condattr_t    : public fibre_condattr_t {};
struct _cfibre_rwlockattr_t  : public fibre_rwlockattr_t {};
struct _cfibre_barrierattr_t : public fibre_barrierattr_t {};

struct _cfibre_fastmutex_t     : public fibre_fastmutex_t {};
struct _cfibre_fastmutexattr_t : public fibre_fastmutexattr_t {};

struct _cfibre_cluster_t    : public Cluster {};
struct _cfibre_eventscope_t : public EventScope {};

extern "C" void cfibre_init() {
  FibreInit();
}

extern "C" void cfibre_init_n(size_t pollerCount, size_t workerCount) {
  FibreInit(pollerCount, workerCount);
}

extern "C" pid_t cfibre_fork() {
  return FibreFork();
}

extern "C" int cfibre_cluster_create(cfibre_cluster_t* cluster) {
  *cluster = new _cfibre_cluster_t;
  return 0;
}

extern "C" int cfibre_cluster_destroy(cfibre_cluster_t* cluster) {
  delete *cluster;
  *cluster = nullptr;
  return 0;
}

extern "C" cfibre_cluster_t cfibre_cluster_self() {
  return reinterpret_cast<_cfibre_cluster_t*>(&Context::CurrCluster());
}

extern "C" int cfibre_add_worker(cfibre_cluster_t cluster, pthread_t* tid, void (*init_routine) (void *), void *arg) {
  *tid = cluster->addWorker(init_routine, arg).getSysID();
  return 0;
}

extern "C" int cfibre_pause(cfibre_cluster_t cluster) {
  cluster->pause();
  return 0;
}

extern "C" int cfibre_resume(cfibre_cluster_t cluster) {
  cluster->resume();
  return 0;
}

extern "C" cfibre_eventscope_t cfibre_clone(void (*mainFunc)(void *), void* mainArg) {
  return reinterpret_cast<_cfibre_eventscope_t*>(Context::CurrEventScope().clone(mainFunc, mainArg));
}

extern "C" cfibre_eventscope_t cfibre_es_self(void) {
  return reinterpret_cast<_cfibre_eventscope_t*>(&Context::CurrEventScope());
}

extern "C" int cfibre_attr_init(cfibre_attr_t *attr) {
  *attr = new _cfibre_attr_t;
  return fibre_attr_init(*attr);
}

extern "C" int cfibre_attr_destroy(cfibre_attr_t *attr) {
  int ret = fibre_attr_destroy(*attr);
  delete *attr;
  *attr = nullptr;
  return ret;
}

extern "C" int cfibre_get_errno() {
  return _SysErrno();
}

extern "C" void cfibre_set_errno(int e) {
  _SysErrnoSet() = e;
}

extern "C" int cfibre_attr_setcluster(cfibre_attr_t *attr, cfibre_cluster_t cluster) {
  return fibre_attr_setcluster(*attr, cluster);
}

extern "C" int cfibre_attr_getcluster(const cfibre_attr_t *attr, cfibre_cluster_t *cluster) {
  return fibre_attr_getcluster(*attr, (Cluster**)cluster);
}

extern "C" int cfibre_attr_setstacksize(cfibre_attr_t *attr, size_t stacksize) {
  return fibre_attr_setstacksize(*attr, stacksize);
}

extern "C" int cfibre_attr_getstacksize(const cfibre_attr_t *attr, size_t *stacksize) {
  return fibre_attr_getstacksize(*attr, stacksize);
}

extern "C" int cfibre_attr_setguardsize(cfibre_attr_t *attr, size_t guardsize) {
  return fibre_attr_setguardsize(*attr, guardsize);
}

extern "C" int cfibre_attr_getguardsize(const cfibre_attr_t *attr, size_t *guardsize) {
  return fibre_attr_getguardsize(*attr, guardsize);
}

extern "C" int cfibre_attr_setpriority(cfibre_attr_t *attr, int priority) {
  return fibre_attr_setpriority(*attr, priority);
}

extern "C" int cfibre_attr_getpriority(const cfibre_attr_t *attr, int *priority) {
  return fibre_attr_getpriority(*attr, priority);
}

extern "C" int cfibre_attr_setaffinity(cfibre_attr_t *attr, int affinity) {
  return fibre_attr_setaffinity(*attr, affinity);
}

extern "C" int cfibre_attr_getaffinity(const cfibre_attr_t *attr, int *affinity) {
  return fibre_attr_getaffinity(*attr, affinity);
}

extern "C" int cfibre_attr_setdetachstate(cfibre_attr_t *attr, int detachstate) {
  return fibre_attr_setdetachstate(*attr, detachstate);
}

extern "C" int cfibre_attr_getdetachstate(const cfibre_attr_t *attr, int *detachstate) {
  return fibre_attr_getdetachstate(*attr, detachstate);
}

extern "C" int cfibre_create(cfibre_t *thread, const cfibre_attr_t *attr, void *(*start_routine) (void *), void *arg) {
  if (attr) {
    return fibre_create((fibre_t*)thread, (fibre_attr_t*)*attr, start_routine, arg);
  } else {
    return fibre_create((fibre_t*)thread, nullptr, start_routine, arg);
  }
}

extern "C" int cfibre_join(cfibre_t thread, void **retval) {
  return fibre_join(thread, retval);
}

extern "C" int cfibre_detach(cfibre_t thread) {
  return fibre_detach(thread);
}

extern "C" void cfibre_exit() {
  fibre_exit();
}

extern "C" cfibre_t cfibre_self(void) {
  return (cfibre_t)fibre_self();
}

extern "C" int cfibre_equal(cfibre_t thread1, cfibre_t thread2) {
  return fibre_equal(thread1, thread2);
}

extern "C" int cfibre_yield(void) {
  return fibre_yield();
}

extern "C" int cfibre_key_create(cfibre_key_t *key, void (*destructor)(void*)) {
  return fibre_key_create(key, destructor);
}

extern "C" int cfibre_key_delete(cfibre_key_t key) {
  return fibre_key_delete(key);
}

extern "C" int cfibre_setspecific(cfibre_key_t key, const void *value) {
  return fibre_setspecific(key, value);
}

extern "C" void *cfibre_getspecific(cfibre_key_t key) {
  return fibre_getspecific(key);
}

extern "C" int cfibre_park(void) {
  return fibre_park();
}

extern "C" int cfibre_unpark(cfibre_t thread) {
  return fibre_unpark(thread);
}

extern "C" int cfibre_migrate(cfibre_cluster_t cluster) {
  return fibre_migrate(cluster);
}

extern "C" int cfibre_sem_init(cfibre_sem_t *sem, int pshared, unsigned int value) {
  *sem = (cfibre_sem_t)new fibre_sem_t;
  return fibre_sem_init(*sem, pshared, value);
}

extern "C" int cfibre_sem_destroy(cfibre_sem_t *sem) {
  int ret = fibre_sem_destroy(*sem);
  delete *sem;
  *sem = nullptr;
  return ret;
}

extern "C" int cfibre_sem_wait(cfibre_sem_t *sem) {
  return fibre_sem_wait(*sem);
}

extern "C" int cfibre_sem_trywait(cfibre_sem_t *sem) {
  return fibre_sem_trywait(*sem);
}

extern "C" int cfibre_sem_timedwait(cfibre_sem_t *sem, const struct timespec *abs_timeout) {
  return fibre_sem_timedwait(*sem, abs_timeout);
}

extern "C" int cfibre_sem_post(cfibre_sem_t *sem) {
  return fibre_sem_post(*sem);
}

extern "C" int cfibre_sem_getvalue(cfibre_sem_t *sem, int *sval) {
  return fibre_sem_getvalue(*sem, sval);
}

extern "C" int cfibre_mutexattr_init(cfibre_mutexattr_t *attr) {
  *attr = new _cfibre_mutexattr_t;
  return fibre_mutexattr_init(*attr);
}

extern "C" int cfibre_mutexattr_destroy(cfibre_mutexattr_t *attr) {
  int ret = fibre_mutexattr_destroy(*attr);
  delete *attr;
  *attr = nullptr;
  return ret;
}

extern "C" int cfibre_mutexattr_settype(cfibre_mutexattr_t *attr, int type) {
  return fibre_mutexattr_settype(*attr, type);
}

extern "C" int cfibre_mutex_init(cfibre_mutex_t *restrict mutex, const cfibre_mutexattr_t *restrict attr) {
  *mutex = (cfibre_mutex_t)new fibre_mutex_t;
  return fibre_mutex_init(*mutex, (fibre_mutexattr_t*)attr);
}

extern "C" int cfibre_mutex_destroy(cfibre_mutex_t *mutex) {
  int ret = fibre_mutex_destroy(*mutex);
  delete *mutex;
  *mutex = nullptr;
  return ret;
}

extern "C" int cfibre_mutex_lock(cfibre_mutex_t *mutex) {
  return fibre_mutex_lock(*mutex);
}

extern "C" int cfibre_mutex_trylock(cfibre_mutex_t *mutex) {
  return fibre_mutex_trylock(*mutex);
}

extern "C" int cfibre_mutex_timedlock(cfibre_mutex_t *restrict mutex, const struct timespec *restrict abstime) {
  return fibre_mutex_timedlock(*mutex, abstime);
}

extern "C" int cfibre_mutex_unlock(cfibre_mutex_t *mutex) {
  return fibre_mutex_unlock(*mutex);
}

extern "C" int cfibre_condattr_init(cfibre_condattr_t *attr) {
  return fibre_condattr_init(*attr);
}

extern "C" int cfibre_condattr_destroy(cfibre_condattr_t *attr) {
  return fibre_condattr_destroy(*attr);
}

extern "C" int cfibre_cond_init(cfibre_cond_t *restrict cond, const cfibre_condattr_t *restrict attr) {
  *cond = (cfibre_cond_t)new fibre_cond_t;
  return fibre_cond_init(*cond, (fibre_condattr_t*)attr);
}

extern "C" int cfibre_cond_destroy(cfibre_cond_t *cond) {
  int ret = fibre_cond_destroy(*cond);
  delete *cond;
  *cond = nullptr;
  return ret;
}

extern "C" int cfibre_cond_wait(cfibre_cond_t *restrict cond, cfibre_mutex_t *restrict mutex) {
  return fibre_cond_wait(*cond, *mutex);
}

extern "C" int cfibre_cond_timedwait(cfibre_cond_t *restrict cond, cfibre_mutex_t *restrict mutex, const struct timespec *restrict abstime) {
  return fibre_cond_timedwait(*cond, *mutex, abstime);
}

extern "C" int cfibre_cond_signal(cfibre_cond_t *cond) {
  return fibre_cond_signal(*cond);
}

extern "C" int cfibre_cond_broadcast(cfibre_cond_t *cond) {
  return fibre_cond_broadcast(*cond);
}

extern "C" int cfibre_rwlock_init(cfibre_rwlock_t *restrict rwlock, const cfibre_rwlockattr_t *restrict attr) {
  *rwlock = (cfibre_rwlock_t)new fibre_rwlock_t;
  return fibre_rwlock_init(*rwlock, (fibre_rwlockattr_t*)attr);
}

extern "C" int cfibre_rwlock_destroy(cfibre_rwlock_t *rwlock) {
  int ret = fibre_rwlock_destroy(*rwlock);
  delete *rwlock;
  *rwlock = nullptr;
  return ret;
}

extern "C" int cfibre_rwlock_rdlock(cfibre_rwlock_t *rwlock) {
  return fibre_rwlock_rdlock(*rwlock);
}

extern "C" int cfibre_rwlock_tryrdlock(cfibre_rwlock_t *rwlock) {
  return fibre_rwlock_tryrdlock(*rwlock);
}

extern "C" int cfibre_rwlock_timedrdlock(cfibre_rwlock_t *restrict rwlock, const struct timespec *restrict abstime) {
  return fibre_rwlock_timedrdlock(*rwlock, abstime);
}

extern "C" int cfibre_rwlock_wrlock(cfibre_rwlock_t *rwlock) {
  return fibre_rwlock_wrlock(*rwlock);
}

extern "C" int cfibre_rwlock_trywrlock(cfibre_rwlock_t *rwlock) {
  return fibre_rwlock_trywrlock(*rwlock);
}

extern "C" int cfibre_rwlock_timedwrlock(cfibre_rwlock_t *restrict rwlock, const struct timespec *restrict abstime) {
  return fibre_rwlock_timedwrlock(*rwlock, abstime);
}

extern "C" int cfibre_rwlock_unlock(cfibre_rwlock_t *rwlock) {
  return fibre_rwlock_unlock(*rwlock);
}

extern "C" int cfibre_barrier_init(cfibre_barrier_t *restrict barrier, const cfibre_barrierattr_t *restrict attr, unsigned count) {
  *barrier = (cfibre_barrier_t)new fibre_barrier_t;
  return fibre_barrier_init(*barrier, (fibre_barrierattr_t*)attr, count);
}

extern "C" int cfibre_barrier_destroy(cfibre_barrier_t *barrier) {
  int ret = fibre_barrier_destroy(*barrier);
  delete *barrier;
  *barrier = nullptr;
  return ret;
}

extern "C" int cfibre_barrier_wait(cfibre_barrier_t *barrier) {
  return fibre_barrier_wait(*barrier);
}

extern "C" int cfibre_fastmutexattr_init(cfibre_fastmutexattr_t *attr) {
  *attr = new _cfibre_fastmutexattr_t;
  return fibre_fastmutexattr_init(*attr);
}

extern "C" int cfibre_fastmutexattr_destroy(cfibre_fastmutexattr_t *attr) {
  int ret = fibre_fastmutexattr_destroy(*attr);
  delete *attr;
  *attr = nullptr;
  return ret;
}

extern "C" int cfibre_fastmutexattr_settype(cfibre_fastmutexattr_t *attr, int type) {
  return fibre_fastmutexattr_settype(*attr, type);
}

extern "C" int cfibre_fastmutex_init(cfibre_fastmutex_t *restrict mutex, const cfibre_fastmutexattr_t *restrict attr) {
  *mutex = (cfibre_fastmutex_t)new fibre_fastmutex_t;
  return fibre_fastmutex_init(*mutex, (fibre_fastmutexattr_t*)attr);
}

extern "C" int cfibre_fastmutex_destroy(cfibre_fastmutex_t *mutex) {
  int ret = fibre_fastmutex_destroy(*mutex);
  delete *mutex;
  *mutex = nullptr;
  return ret;
}

extern "C" int cfibre_fastmutex_lock(cfibre_fastmutex_t *mutex) {
  return fibre_fastmutex_lock(*mutex);
}

extern "C" int cfibre_fastmutex_trylock(cfibre_fastmutex_t *mutex) {
  return fibre_fastmutex_trylock(*mutex);
}

extern "C" int cfibre_fastmutex_unlock(cfibre_fastmutex_t *mutex) {
  return fibre_fastmutex_unlock(*mutex);
}

extern "C" int cfibre_fastcond_wait(cfibre_cond_t *restrict cond, cfibre_fastmutex_t *restrict mutex) {
  return fibre_fastcond_wait(*cond, *mutex);
}

extern "C" int cfibre_fastcond_timedwait(cfibre_cond_t *restrict cond, cfibre_fastmutex_t *restrict mutex, const struct timespec *restrict abstime) {
  return fibre_fastcond_timedwait(*cond, *mutex, abstime);
}

#if defined(__linux__)
extern "C" int cfibre_epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout) {
  return lfEpollWait(epfd, events, maxevents, timeout);
}
#endif

extern "C" int cfibre_socket(int domain, int type, int protocol) {
  return lfSocket(domain, type, protocol);
}

extern "C" int cfibre_bind(int socket, const struct sockaddr *address, socklen_t address_len) {
  return lfBind(socket, address, address_len);
}

extern "C" int cfibre_listen(int socket, int backlog) {
  return lfListen(socket, backlog);
}

extern "C" int cfibre_accept(int socket, struct sockaddr *restrict address, socklen_t *restrict address_len) {
  return lfAccept(socket, address, address_len, 0);
}

extern "C" int cfibre_accept4(int socket, struct sockaddr *restrict address, socklen_t *restrict address_len, int flags) {
  return lfAccept(socket, address, address_len, flags);
}

extern "C" int cfibre_connect(int socket, const struct sockaddr *address, socklen_t address_len) {
  return lfConnect(socket, address, address_len);
}

extern "C" int cfibre_dup(int fd) {
  return lfDup(fd);
}

extern "C" int cfibre_pipe(int pipefd[2]) {
  return lfPipe(pipefd);
}

extern "C" int cfibre_pipe2(int pipefd[2], int flags) {
  return lfPipe(pipefd, flags);
}

extern "C" int cfibre_close(int fd) {
  return lfClose(fd);
}

extern "C" ssize_t cfibre_send(int socket, const void *buffer, size_t length, int flags) {
  return lfSend(socket, buffer, length, flags);
}

extern "C" ssize_t cfibre_sendto(int socket, const void *message, size_t length, int flags, const struct sockaddr *dest_addr, socklen_t dest_len) {
  return lfSendto(socket, message, length, flags, dest_addr, dest_len);
}

extern "C" ssize_t cfibre_sendmsg(int socket, const struct msghdr *message, int flags) {
  return lfSendmsg(socket, message, flags);
}

extern "C" ssize_t cfibre_write(int fildes, const void *buf, size_t nbyte) {
  return lfWrite(fildes, buf, nbyte);
}

extern "C" ssize_t cfibre_writev(int fildes, const struct iovec *iov, int iovcnt) {
  return lfWritev(fildes, iov, iovcnt);
}

extern "C" ssize_t cfibre_recv(int socket, void *buffer, size_t length, int flags) {
  return lfRecv(socket, buffer, length, flags);
}

extern "C" ssize_t cfibre_recvfrom(int socket, void *restrict buffer, size_t length, int flags, struct sockaddr *restrict address, socklen_t *restrict address_len) {
  return lfRecvfrom(socket, buffer, length, flags, address, address_len);
}

extern "C" ssize_t cfibre_recvmsg(int socket, struct msghdr *message, int flags) {
  return lfRecvmsg(socket, message, flags);
}

extern "C" ssize_t cfibre_read(int fildes, void *buf, size_t nbyte) {
  return lfRead(fildes, buf, nbyte);
}

extern "C" ssize_t cfibre_readv(int fildes, const struct iovec *iov, int iovcnt) {
  return lfReadv(fildes, iov, iovcnt);
}

#if defined(__FreeBSD__)
extern "C" int cfibre_sendfile(int fd, int s, off_t offset, size_t nbytes, struct sf_hdtr *hdtr, off_t *sbytes, int flags) {
  return lfOutput(sendfile, fd, s, offset, nbytes, hdtr, sbytes, flags);
}
#else
extern "C" ssize_t cfibre_sendfile(int out_fd, int in_fd, off_t *offset, size_t count) {
  return lfOutput(sendfile, out_fd, in_fd, offset, count);
}
#endif

extern "C" int cfibre_fcntl(int fildes, int cmd, int flags) {
  return lfFcntl(fildes, cmd, flags);
}

extern "C" int cfibre_nanosleep(const struct timespec *rqtp, struct timespec*) {
  assert(rqtp);
  Fibre::nanosleep(*rqtp);
  return 0;
}

extern "C" int cfibre_usleep(useconds_t usec) {
  Fibre::usleep(usec);
  return 0;
}

extern "C" int cfibre_sleep(unsigned int seconds) {
  Fibre::sleep(seconds);
  return 0;
}
