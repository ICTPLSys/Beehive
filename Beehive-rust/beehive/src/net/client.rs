use crate::mem::RemoteAddr;

use super::config::Config;
use super::ibverbs::*;
use super::memory::{allocate_memory, deallocate_memory};
use super::rdma::{self, ClientInfo, QPInfo, ServerInfo, ibv_poll_cq};
use log::{error, info};
use rand::RngCore;
use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::net::TcpStream;
use std::ptr;
use std::vec::Vec;

/// Client manages rdma connections and context.
///
/// It should be a singleton.
pub struct Client {
    memory: *mut c_void,
    ctx: *mut ibv_context,
    pd: *mut ibv_pd,
    control_cq: *mut ibv_cq,
    data_cq: *mut ibv_cq,
    mr: *mut ibv_mr,
    lkey: u32,
    conns: Vec<ConnWithServer>,
    config: Config,
}

struct ConnWithServer {
    control_qp: *mut ibv_qp,
    data_qps: Vec<*mut ibv_qp>, // one for each core
    remote_base_addr: u64,
    remote_key: u32,
}

impl Client {
    const SERVER_IDX_SHIFT: u32 = 48;
    const REMOTE_ADDR_MASK: u64 = (1 << Self::SERVER_IDX_SHIFT) - 1;

    /// check if the singleton instance is initialzied
    pub fn initialized() -> bool {
        !Self::instance().memory.is_null()
    }

    /// initialize the singleton instance
    pub fn initialize(config: &Config) {
        info!("initializing rdma client");
        unsafe {
            let instance_ptr = CLIENT_INSTANCE.0.get();
            (*instance_ptr).write(Self::new(config)).connect_all();
        }
        info!("rdma client initialized");
    }

    /// destroy the singleton instance
    pub fn destroy() {
        info!("destroying rdma client");
        assert!(Self::initialized());
        unsafe {
            let instance_ptr = CLIENT_INSTANCE.0.get();
            (*instance_ptr).assume_init_drop();
        }
        info!("rdma client deinitialized");
    }

    pub fn post_read(remote_addr: RemoteAddr, local_addr: u64, length: u32, wr_id: u64) -> bool {
        let server_idx = remote_addr.server_id() as usize;
        let remote_offset = remote_addr.offset();
        let remote_addr = Self::instance().conns[server_idx].remote_base_addr + remote_offset;
        let tidx = libfibre_port::cfibre_thread_idx();
        let conn = &Self::instance().conns[server_idx as usize];
        unsafe {
            rdma::post_read_signal(
                conn.data_qps[tidx],
                Self::instance().lkey,
                conn.remote_key,
                remote_addr,
                local_addr,
                length,
                wr_id,
            )
        }
    }

    pub fn post_write(remote_addr: RemoteAddr, local_addr: u64, length: u32, wr_id: u64) -> bool {
        let server_idx = remote_addr.server_id() as usize;
        let remote_offset = remote_addr.offset();
        let tidx = libfibre_port::cfibre_thread_idx();
        debug_assert!(tidx < Self::instance().config.num_cores);
        let conn = &Self::instance().conns[server_idx];
        let remote_addr = conn.remote_base_addr + remote_offset;
        unsafe {
            rdma::post_write_signal(
                conn.data_qps[tidx],
                Self::instance().lkey,
                conn.remote_key,
                remote_addr,
                local_addr,
                length,
                wr_id,
            )
        }
    }

    // TODO: batched write

    pub fn poll<Fr, Fw>(on_read_complete: Fr, on_write_complete: Fw) -> usize
    where
        Fr: Fn(u64),
        Fw: Fn(u64),
    {
        const CHECK_CQ_BATCH_SIZE: usize = 32; // TODO: can this be configurable?
        let mut wcs: MaybeUninit<[ibv_wc; CHECK_CQ_BATCH_SIZE]> = MaybeUninit::uninit();
        let n = unsafe {
            rdma::ibv_poll_cq(
                Self::instance().data_cq,
                CHECK_CQ_BATCH_SIZE as i32,
                wcs.assume_init_mut().as_mut_ptr(),
            )
        };
        assert!(n >= 0);
        let n = n as usize;
        debug_assert!(n <= CHECK_CQ_BATCH_SIZE);
        let valid_wcs = unsafe { &wcs.assume_init_ref()[0..n] };
        for wc in valid_wcs {
            assert_eq!(wc.status, ibv_wc_status::IBV_WC_SUCCESS);
            match wc.opcode {
                ibv_wc_opcode::IBV_WC_RDMA_READ => on_read_complete(wc.wr_id),
                ibv_wc_opcode::IBV_WC_RDMA_WRITE => on_write_complete(wc.wr_id),
                _ => error!("unexpected rdma wc opcode {}", wc.opcode.0),
            }
        }
        n
    }

    pub unsafe fn get_memory() -> *mut c_void {
        debug_assert!(Self::initialized());
        Self::instance().memory
    }

    pub unsafe fn get_remote_base_addr(i: usize) -> u64 {
        debug_assert!(Self::initialized());
        Self::instance().conns[i].remote_base_addr
    }

    pub fn get_heap_size() -> usize {
        debug_assert!(Self::initialized());
        Self::instance().config.client_memory_size
    }

    pub fn instance() -> &'static Client {
        unsafe { (*(CLIENT_INSTANCE.0.get())).assume_init_ref() }
    }

    fn new(config: &Config) -> Client {
        unsafe {
            let memory = allocate_memory(config.client_memory_size);
            let ctx = open_ib_device();
            let pd = ibv_alloc_pd(ctx);
            let mr = register_mr(pd, memory, config.client_memory_size);
            let control_cq =
                ibv_create_cq(ctx, config.cq_entries, ptr::null_mut(), ptr::null_mut(), 0);
            let data_cq =
                ibv_create_cq(ctx, config.cq_entries, ptr::null_mut(), ptr::null_mut(), 0);
            Client {
                memory,
                ctx,
                pd,
                control_cq,
                data_cq,
                mr,
                lkey: (*mr).lkey,
                conns: vec![],
                config: config.clone(),
            }
        }
    }

    fn connect(&mut self, idx: usize) -> ConnWithServer {
        // create QPs
        log::info!("connect start");
        let num_data_qps = self.config.num_cores;
        let num_qps = num_data_qps + 1;
        let mut control_qp =
            unsafe { rdma::init_queue_pair(self.ctx, self.control_cq, self.pd, &self.config) };
        let mut data_qps: Vec<_> = (0..num_data_qps)
            .map(|_| unsafe {
                rdma::init_queue_pair(self.ctx, self.data_cq, self.pd, &self.config)
            })
            .collect();

        // query client info
        let mut rng = rand::thread_rng();
        let client_info = ClientInfo {
            server_memory_size: self.config.servers[idx].memory_size,
            lid: unsafe { query_lid(self.ctx, self.config.ib_port) },
            qp_count: num_qps as u16,
            mtu: self.config.qp_mtu,
            max_rd_atomic: self.config.qp_max_rd_atomic,
        };
        let client_qp_info: Vec<_> = std::iter::once(&control_qp)
            .chain(data_qps.iter())
            .map(|qp| QPInfo {
                qpn: unsafe { (*(*qp)).qp_num },
                psn: rng.next_u32(),
            })
            .collect();

        // exchange info
        let addr = self.config.servers[idx].addr.as_str();
        let mut stream = TcpStream::connect(addr).unwrap();
        info!("connect!");
        bincode::serialize_into(&mut stream, &(client_info, client_qp_info)).unwrap();
        let (server_info, server_qp_info): (ServerInfo, Vec<QPInfo>) =
            bincode::deserialize_from(&mut stream).unwrap();
        info!("deserialize");
        assert!(server_qp_info.len() == num_qps);

        // set up QPs
        std::iter::once(&mut control_qp)
            .chain(data_qps.iter_mut())
            .zip(server_qp_info.iter())
            .for_each(|(qp, info)| unsafe {
                rdma::modify_qp_rtr(*qp, server_info.lid, info.psn, info.qpn, &self.config);
                rdma::modify_qp_rts(*qp, info.psn, &self.config);
            });

        ConnWithServer {
            control_qp,
            data_qps,
            remote_base_addr: server_info.addr,
            remote_key: server_info.rkey,
        }
    }

    fn connect_all(&mut self) {
        let num_servers = self.config.servers.len();
        log::info!("try to connect to {num_servers} servers");
        self.conns = (0..num_servers).map(|idx| self.connect(idx)).collect();
    }

    fn post_stop(&self) {
        for conn in &self.conns {
            let mut wc = std::mem::MaybeUninit::uninit();
            loop {
                if unsafe {
                    rdma::post_send(
                        ibv_wr_opcode::IBV_WR_SEND,
                        ibv_send_flags::IBV_SEND_SIGNALED,
                        conn.control_qp,
                        self.lkey,
                        conn.remote_key,
                        0,
                        0,
                        0,
                        rdma::RQ_STOP,
                    )
                } {
                    break;
                } else {
                    unsafe {
                        ibv_poll_cq(self.control_cq, 1, wc.as_mut_ptr());
                    }
                }
            }
            loop {
                let n = unsafe { ibv_poll_cq(self.control_cq, 1, wc.as_mut_ptr()) };
                assert!(n <= 1);
                if n == 1 {
                    let wc = unsafe { wc.assume_init() };
                    if wc.wr_id == rdma::RQ_STOP && wc.opcode == ibv_wc_opcode::IBV_WC_SEND {
                        assert_eq!(wc.status, ibv_wc_status::IBV_WC_SUCCESS);
                        break;
                    }
                }
            }
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.post_stop();
        unsafe {
            for conn in self.conns.iter_mut() {
                let err = ibv_destroy_qp(conn.control_qp);
                assert_eq!(err, 0);
                for qp in conn.data_qps.iter_mut() {
                    let err = ibv_destroy_qp(*qp);
                    assert_eq!(err, 0);
                }
            }
            let err = ibv_destroy_cq(self.control_cq);
            assert_eq!(err, 0);
            let err = ibv_destroy_cq(self.data_cq);
            assert_eq!(err, 0);
            let err = ibv_dereg_mr(self.mr);
            assert_eq!(err, 0);
            let err = ibv_dealloc_pd(self.pd);
            assert_eq!(err, 0);
            let err = ibv_close_device(self.ctx);
            assert_eq!(err, 0);
            deallocate_memory(self.memory, self.config.client_memory_size);
            self.memory = ptr::null_mut();
        }
    }
}

// This is to build a singleton for Client
struct ClientCell(UnsafeCell<MaybeUninit<Client>>);
unsafe impl Sync for ClientCell where Client: Sync {}
static CLIENT_INSTANCE: ClientCell = ClientCell(UnsafeCell::new(MaybeUninit::zeroed()));

// All ibverbs api are thread safe
unsafe impl Sync for Client {}
