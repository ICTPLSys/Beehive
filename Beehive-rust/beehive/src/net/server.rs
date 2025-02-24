use super::config::Config;
use super::ibverbs::*;
use super::memory::{allocate_memory, deallocate_memory};
use super::rdma::{ClientInfo, QPInfo, ServerInfo, init_queue_pair, modify_qp_rtr};
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
        // TODO
        let size = config.servers[0].memory_size;
        unsafe {
            let memory = allocate_memory(size);
            let ctx = open_ib_device();
            let pd = ibv_alloc_pd(ctx);
            let mr = register_mr(pd, memory, config.client_memory_size);
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
        // TODO
        let addr = self.config.servers[0].addr.as_str();

        // listen for client
        let listener = TcpListener::bind(addr).unwrap();
        let mut stream = listener.incoming().next().unwrap().unwrap();
        let (client_info, client_qp_info): (ClientInfo, Vec<QPInfo>) =
            bincode::deserialize_from(&mut stream).unwrap();

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
        let server_qp_info: Vec<_> = self
            .qps
            .iter()
            .map(|qp| unsafe { (*(*qp)).qp_num })
            .zip(client_qp_info.iter().map(|info| info.psn))
            .collect();
        bincode::serialize_into(&mut stream, &(server_info, server_qp_info)).unwrap();

        // set up QPs
        client_qp_info
            .iter()
            .zip(self.qps.iter())
            .for_each(|(info, qp)| unsafe {
                modify_qp_rtr(*qp, client_info.lid, info.psn, info.qpn, &self.config);
            });
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        unsafe {
            deallocate_memory(self.memory, self.size);
        }
    }
}
