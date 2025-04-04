# EPOLLONDEMAND Documentation

Zhaocheng Che (z4che|**at**|uwaterloo.ca)

## Content

- Usage
- Implementation
	- `ep_poll_callback`
	- `ep_send_events`
- Appendix - Call Stack
- Reference

## Usage

`epoll_ctl(
		epfd, 
		EPOLL_CTL_ADD,
		... | EPOLLONESHOT | EPOLLONDEMAND
)`

When `EPOLLONDEMAND` bit is set, if some socket receive syscall
returns an error code `EAGAIN`, then every epoll item on this fd
will be examined before the syscall returns. Each epoll item that 
was deactivated by `EPOLLONESHOT` will be rearmed.

The design rationale is that in the common usage of `EPOLLONESHOT`,
the socket's epoll item is usually deactivated, and until a receive
syscall returns an `EAGAIN` error code, a 
`epoll_ctl( ,EPOLL_CTL_MOD, )` follows to rearm the epoll item. [1]

This new `EPOLLONDEMAND` bit saves the `epoll_ctl( ,EPOLL_CTL_MOD, )`
by performing it within the socket receive syscall. The intended
improvements are 1) overhead of system call, 2) unnecessary locking
in a generic `epoll_ctl( ,EPOLL_CTL_MOD, )`, and 3) overhead to look
up the epoll item from the `epfd`'s rbtree.

ATTENTION : If an epoll item is created with `EPOLLONDEMAND`, then
`epoll_ctl( ,EPOLL_CTL_MOD, )` should not be invoked on this epoll
item. Otherwise, the effect may be lost due to a concurrent rearm
in socket receive syscall.

## Implementation

When a socket receive syscall will return an `EAGAIN` code, it calls
`sock_in_ondemand()`. Similar to `sock_def_readable()`, it locks and 
loops the waitqueue, and for each entry in the waitqueue, 
`ep_modify_ondemand()` is invoked.

`ep_modify_ondemand()` is a customized(simplified) version of 
`ep_modify()` but it doesn't hold `ep->mtx`, because it doesn't change 
`epi->event.data`. This also means that it is not mutual exclusive with
`ep_modify()`, so `epoll_ctl( ,EPOLL_CTL_MOD, )` should not be used
with `EPOLLONDEMAND`.

Firstly, the change of `EPOLLWAKEUP` can be removed, since this flag
is part of `EP_PRIVATE_BITS` and the wakeupsource doesn't change
when the `epitem` is disabled in `ep_send_events`.

Secondly, because `ep_modify_ondemand()` is invoked while holding the
waitqueue spinlock, so `lockdep_assert_irqs_enabled()` should also
be removed.

The following text discusses the synchronization with other routines
that read/write this `epitem`.

### `ep_poll_callback`

This callback function is invoked in some 'for each' loop
of `sk->sk_wq->wait`, e.g. `sock_def_readable()`. So it is invoked
while holding the spinlock of the waitqueue, which provides mutual
exclusion with `ep_modify_ondemand()`.

This mutual exclusion simplifies the possible cases when publishing
the rearm of this `epitem`:

	We take the socket state after the wait queue is locked. We could
	capture some socket-state-changes relevant to the rearmed `epitem`.
	No matter the corresponding `ep_poll_callback`s are before or after
	the `ep_modify_ondemand`s, we always examine the state with the 
	rearmed events. Thus, any past socket-state-changes are not missed.

	There could also be socket-state-changes while `ep_modify_ondemand`s 
	executes, then the corresponding `ep_poll_callback`s must happen 
	after the `ep_modify_ondemand`s finishes. At that time, the rearm 
	is published, and the callbacks will not miss those events.

Looking at the `smp_mb()` between the rearm of events and reexamine 
the socket-state, in `ep_modify()`, it serves two purposes (in the 
comments) : 1) publish the rearm to concurrent `ep_poll_callback`s,
and 2) pairs with the barrier in wq_has_sleeper. 

In `ep_modify_ondemand`, provided with mutual exclusion with
`ep_poll_callback`, the first purpose is no longer needed.

The second purpose is to ensure that `->poll()` happens after rearming
the `epitem`. Because otherwise this could happen in `ep_modify()`.

	     Operation                  epitem status
	============================================
	t1   ep_modify                  disabled
	t1   ->poll                     disabled
	t2   socket receive data
	t2   ep_poll_callback           disabled
	t1   ep_modify change events    rearmed
	t2   ep_modify exits            rearmed

In the end, `ep_modify` fails to detect the socket's status, and
missed an event. However, because `ep_modify_ondemand()` has mutual
exclusion with `ep_poll_callback()`, the callback will be delayed until
`ep_modify_ondemand()` exits, at which point the `epitem` is rearmed,
and callback will put it on the readylist. Thus, events will not be
missed here.

In conclusion, `smp_mb()` is not needed in `ep_modify_ondemand()`.

Therefor, we can further optimize the current code by only
polling the socket-status once for all `epitem` on its waitqueue, 
right after locking the waitqueue, and pass the `__poll_t` as a
parameter to `ep_modify_ondemand`.

### `ep_send_events`

When an `epitem` with `EPOLLONESHOT` property is transfered from
the ready list to the userspace via `epoll_wait` syscall, 
`ep_send_events` will disable the `epitem`. There could be overlap
between the modify in `ep_send_events` and read & modify in 
`ep_modify_ondemand`.

However, the disable in `ep_send_events` also means that the `epitem`
is send to the `epoll_wait` syscall. So the user knows that the
socket is possible to be ready. In this case, whether the `epitem`
is rearmed is not important, because the information is already 
delivered. In the common usage, the socket receive function will be
invoked by the user. Therefore, the race condition won't result
in a lost signal.

For example,

	     Operation                  epitem status
	============================================
	t1   socket-receive EAGAIN      active
	t2   data arrive at socket
	t2   ep_poll_callback           active & readylist
	t1   ep_modify_ondemand         active & readylist
	t2   ep_send_events             disabled

Even though the socket-receive returns `EAGAIN` and the `epitem` is
disbled, and it seems that a signal is lost. However, the information
of readiness is already delivered to the user by `ep_send_events`.
So possibly a socket-receive will be called again, which 
either succeed,
or `EAGAIN` and rearm the `epitem`,
or the same outcome and will retry.

Less importantly, the race condition could result in a falsely rearmed
`epitem`. For example, t1 means thread 1, t2 means thread 2,

	     Operation                  epitem status
	============================================
	t1   socket-receive EAGAIN      active
	t2   data arrive at socket
	t2   ep_poll_callback           active & readylist
	t2   ep_send_events             disabled
	t1   ep_modify_ondemand         active

Now the `epitem` is active. However, this problem is usually not a 
big issue because as soon as `ep_send_events` executes without 
interference, the `epitem` will be disabled to prevent spurious 
wakeup. If socket-receive starts with a disabled `eptime`, then
this false rearm won't occur.

## Appendix - Call Stack

1. ep_modify_ondemand

	recv() / read() / ...
		sock_recvmsg_nosec()
			sock_in_ondemand()
				ep_modify_ondemand()

2. ep_poll_callback

	sk->sk_data_ready = sock_def_readable; [from sock_init_data()]
	sock_def_readable()
		wake_up_interruptible_sync_poll()
			__wake_up_sync_key()
				__wake_up_common_lock()
					__wake_up_common()
						curr->func = ep_poll_callback; [from ep_ptable_queue_proc()]

3. ep_send_events

	epoll_wait()
		do_epoll_wait()
			ep_poll()
				ep_send_events()


## Reference

[1] https://man7.org/linux/man-pages/man2/epoll_ctl.2.html  
[2] https://www.kernel.org/doc/Documentation/memory-barriers.txt  
[3] https://patchwork.ozlabs.org/project/netdev/patch/1357148750.21409.17169.camel@edumazet-glaptop/

