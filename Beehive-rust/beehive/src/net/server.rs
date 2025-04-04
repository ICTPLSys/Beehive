use super::config::Config;
use super::ibverbs::*;
use super::memory::{allocate_memory, deallocate_memory};
use super::rdma::{
    ClientInfo, QPInfo, RQ_STOP, ServerInfo, ibv_poll_cq, init_queue_pair, modify_qp_rtr,
};
use log::info;
use std::ffi::c_void;
use std::net::TcpListener;
use std::ptr;

/// the memory server
pub struct Server {
    memory: *mut c_void,
    size: usize,
    ctx: *mut ibv_context,
    pd: *mut ibv_pd,
    cq: *mut ibv_cq,
    mr: *mut ibv_mr,
    qps: Vec<*mut ibv_qp>,
    config: Config,
}

impl Server {
    pub fn new(config: Config) -> Server {
        let size = config.servers[0].memory_size;
        unsafe {
            let memory = allocate_memory(size);
            let ctx = open_ib_device();
            let pd = ibv_alloc_pd(ctx);
            let mr = register_mr(pd, memory, size);
            let cq = ibv_create_cq(ctx, config.cq_entries, ptr::null_mut(), ptr::null_mut(), 0);
            Server {
                memory,
                size,
                ctx,
                pd,
                cq,
                mr,
                qps: vec![],
                config,
            }
        }
    }

    pub fn connect(&mut self) {
        let addr = self.config.servers[0].addr.as_str();

        // listen for client
        let listener = TcpListener::bind(addr).unwrap();
        let mut stream = listener.incoming().next().unwrap().unwrap();
        let (client_info, client_qp_info): (ClientInfo, Vec<QPInfo>) =
            bincode::deserialize_from(&mut stream).unwrap();
        info!("deserialize");
        // check and set up config
        assert!(self.size >= client_info.server_memory_size);
        self.config.qp_mtu = client_info.mtu;
        self.config.qp_max_rd_atomic = client_info.max_rd_atomic;

        // create QPs
        let num_qps = client_info.qp_count as usize;
        assert!(client_qp_info.len() == num_qps);
        self.qps = (0..num_qps)
            .map(|_| unsafe { init_queue_pair(self.ctx, self.cq, self.pd, &self.config) })
            .collect();

        // query & send server info
        let server_info = ServerInfo {
            lid: unsafe { query_lid(self.ctx, self.config.ib_port) },
            qp_count: num_qps as u16,
            rkey: (unsafe { *self.mr }).rkey,
            addr: self.memory as u64,
        };
        log::info!("server memory addr: {}", server_info.addr);
        log::info!("server memory size: {}", self.size);
        let server_qp_info: Vec<_> = self
            .qps
            .iter()
            .map(|qp| unsafe { (*(*qp)).qp_num })
            .zip(client_qp_info.iter().map(|info| info.psn))
            .collect();
        bincode::serialize_into(&mut stream, &(server_info, server_qp_info)).unwrap();
        info!("serialize");

        // set up QPs
        client_qp_info
            .iter()
            .zip(self.qps.iter())
            .for_each(|(info, qp)| unsafe {
                modify_qp_rtr(*qp, client_info.lid, info.psn, info.qpn, &self.config);
            });
        info!("Connected!");
        self.wait_for_stop();
    }

    fn wait_for_stop(&self) {
        let mut recv_stop_wr: ibv_recv_wr =
            unsafe { std::mem::MaybeUninit::uninit().assume_init() };
        recv_stop_wr.wr_id = RQ_STOP;
        recv_stop_wr.next = std::ptr::null_mut();
        recv_stop_wr.sg_list = std::ptr::null_mut();
        recv_stop_wr.num_sge = 0;
        let mut bad_wr: *mut ibv_recv_wr = std::ptr::null_mut();
        let control_qp = self.qps[0];
        let post_recv = unsafe { (*(*control_qp).context).ops.post_recv.unwrap_unchecked() };
        unsafe {
            post_recv(control_qp, &mut recv_stop_wr, &mut bad_wr);
        }
        loop {
            let mut wc = unsafe { std::mem::MaybeUninit::uninit().assume_init() };
            let poll_ret = unsafe { ibv_poll_cq(self.cq, 1, &mut wc) };
            debug_assert!(poll_ret <= 1);
            if poll_ret == 1 {
                assert_eq!(wc.wr_id, RQ_STOP);
                assert_eq!(wc.status, ibv_wc_status::IBV_WC_SUCCESS);
                info!("server stoped");
                break;
            }
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        unsafe {
            deallocate_memory(self.memory, self.size);
        }
    }
}
