use std::time::Instant;
const THREAD_NUM: usize = 16;
const TEST_COUNT: usize = 100000;
use beehive::thread::fork_join_task;
use futures::stream::StreamExt;
use libfibre_port::*;
use rayon::iter::*;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic;
use std::task::{Context, Poll, Waker};
struct NPending {
    max: usize,
    current: atomic::AtomicUsize,
}

impl NPending {
    fn new(n: usize) -> Self {
        Self {
            max: n,
            current: atomic::AtomicUsize::new(0),
        }
    }
}

impl Future for NPending {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if self.current.load(atomic::Ordering::Relaxed) < self.max {
            let this = self.get_mut();
            // simulate the cost of atomic operation
            this.current
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            std::hint::black_box(&this);
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

async fn foo(n: usize) {
    let task = NPending::new(n);
    task.await;
}

fn run(n: usize, cx: &mut Context<'_>) {
    let mut count = 0;
    let mut fut = pin! { foo(n) };
    loop {
        match fut.as_mut().poll(cx) {
            Poll::Pending => count += 1,
            Poll::Ready(()) => break,
        }
    }
    assert_eq!(count, n);
}

fn profile_vanilla_async_yield() {
    async_std::task::block_on(async {
        let start = Instant::now();
        let mut vec = Vec::with_capacity(THREAD_NUM);
        for _ in 0..THREAD_NUM {
            vec.push(async_std::task::spawn(async {
                for _ in 0..TEST_COUNT {
                    async_std::task::yield_now().await;
                }
            }));
        }
        for handle in vec {
            handle.await;
        }
        let end = Instant::now();
        println!(
            "async_std_yield: {:?}",
            (end - start).div_f64((THREAD_NUM * TEST_COUNT) as f64)
        );
    });
}

fn profile_tokio_yield() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let start = Instant::now();
        let mut vec = Vec::with_capacity(THREAD_NUM);
        for _ in 0..THREAD_NUM {
            vec.push(tokio::spawn(async {
                for _ in 0..TEST_COUNT {
                    tokio::task::yield_now().await;
                }
            }));
        }
        for handle in vec {
            handle.await.unwrap();
        }
        let end = Instant::now();
        println!(
            "tokio_yield: {:?}",
            (end - start).div_f64((THREAD_NUM * TEST_COUNT) as f64)
        );
    });
}

fn profile_thread_yield() {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let start = Instant::now();
    pool.install(|| {
        (0..THREAD_NUM)
            .into_par_iter()
            .map(|_| {
                for _ in 0..TEST_COUNT {
                    std::thread::yield_now();
                }
            })
            .collect::<Vec<_>>();
    });
    let end = Instant::now();
    println!(
        "thread_yield: {:?}",
        (end - start).div_f64((THREAD_NUM * TEST_COUNT) as f64)
    );
}

fn profile_uthread_yield() {
    cfibre_init(1);
    let start = Instant::now();
    fork_join_task(THREAD_NUM, THREAD_NUM * TEST_COUNT, |_| {
        libfibre_port::yield_now();
    });
    let end = Instant::now();
    println!(
        "uthread_yield: {:?}",
        (end - start).div_f64((THREAD_NUM * TEST_COUNT) as f64)
    );
}

fn profile_manual_async_yield() {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(&waker);

    let start = Instant::now();
    run(THREAD_NUM * TEST_COUNT, &mut cx);
    let end = Instant::now();
    println!(
        "manual_async_yield: {:?}",
        (end - start).div_f64((THREAD_NUM * TEST_COUNT) as f64)
    );
}

fn profile_tokio_stream_yield() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let start = Instant::now();
        let mut stream = futures::stream::iter(0..THREAD_NUM).then(|_| {
            Box::pin(async {
                for _ in 0..TEST_COUNT {
                    tokio::task::yield_now().await;
                }
            })
        });
        while let Some(_) = stream.next().await {}
        let end = Instant::now();
        println!(
            "tokio_stream_yield: {:?}",
            (end - start).div_f64((THREAD_NUM * TEST_COUNT) as f64)
        );
    });
}

fn profile_smol_yield() {
    smol::block_on(async {
        let start = Instant::now();
        let mut vec = Vec::with_capacity(THREAD_NUM);
        for _ in 0..THREAD_NUM {
            vec.push(smol::spawn(async {
                for _ in 0..TEST_COUNT {
                    smol::future::yield_now().await;
                }
            }));
        }
        for handle in vec {
            handle.await;
        }
        let end = Instant::now();
        println!(
            "smol_yield: {:?}",
            (end - start).div_f64((THREAD_NUM * TEST_COUNT) as f64)
        );
    });
}

fn main() {
    profile_thread_yield();
    profile_vanilla_async_yield();
    profile_tokio_yield();
    profile_uthread_yield();
    profile_tokio_stream_yield();
    profile_smol_yield();
    profile_manual_async_yield();
}
