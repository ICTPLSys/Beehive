use futures::future::join_all;
use nix::libc::gettid;
static THREAD_ID_COUNTER: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);
pub const MAX_THREAD_COUNT: usize = 256;
pub static mut THREAD_TIDS: [i32; MAX_THREAD_COUNT] = [-1; MAX_THREAD_COUNT];

#[derive(Debug)]
pub struct ThreadId {
    id: i32,
    tid: i32,
}

impl ThreadId {
    const INVALID_ID: i32 = -1;

    pub fn get_id(&self) -> i32 {
        self.id
    }

    pub fn get_tid(&self) -> i32 {
        self.tid
    }

    pub fn init(&mut self) {
        let id: i32 = THREAD_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tid: i32;
        unsafe {
            tid = gettid();
            THREAD_TIDS[id as usize] = tid;
        }
        self.id = id;
        self.tid = tid;
    }
}

#[thread_local]
static mut THREAD_ID: ThreadId = ThreadId {
    id: ThreadId::INVALID_ID,
    tid: ThreadId::INVALID_ID,
};

#[allow(static_mut_refs)]
pub fn get_thread_idx() -> i32 {
    unsafe { THREAD_ID.get_id() }
}

#[allow(static_mut_refs)]
pub fn get_thread_tid() -> i32 {
    unsafe { THREAD_ID.get_tid() }
}

#[allow(static_mut_refs)]
pub fn init_thread_id() {
    unsafe {
        THREAD_ID.init();
    }
}

pub fn fork_join<F, Fut>(rt: &tokio::runtime::Runtime, task_num: u64, func: F) -> ()
where
    F: (Fn() -> Fut) + Send + Clone + 'static,
    Fut: Future<Output = ()> + Send,
{
    let mut handles = vec![];
    for _ in 0..task_num {
        let f = func.clone();
        let handle = rt.spawn(async move {
            f().await;
        });
        handles.push(handle);
    }
    rt.block_on(async {
        let hs = join_all(handles).await;
        for h in hs {
            h.expect("err");
        }
    });
}
