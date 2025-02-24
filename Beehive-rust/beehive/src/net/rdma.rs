use std::{mem::MaybeUninit, os::raw::c_void, ptr};

use super::config::Config;
use super::ibverbs::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ClientInfo {
    pub server_memory_size: usize,
    pub lid: u16,
    pub qp_count: u16,
    pub mtu: u32,
    pub max_rd_atomic: u8,
}

#[derive(Serialize, Deserialize)]
pub struct ServerInfo {
    pub lid: u16,
    pub qp_count: u16,
    pub rkey: u32,
    pub addr: u64,
}

#[derive(Serialize, Deserialize)]
pub struct QPInfo {
    pub qpn: u32,
    pub psn: u32,
}

/// initialize a queue pair and set its state to INIT
pub unsafe fn init_queue_pair(
    ctx: *mut ibv_context,
    cq: *mut ibv_cq,
    pd: *mut ibv_pd,
    config: &Config,
) -> *mut ibv_qp {
    let qp_cap = ibv_qp_cap {
        max_send_wr: config.qp_send_cap,
        max_recv_wr: config.qp_recv_cap,
        max_send_sge: config.qp_max_send_sge,
        max_recv_sge: config.qp_max_recv_sge,
        max_inline_data: 0,
    };
    let mut qp_init_attr = ibv_qp_init_attr {
        qp_context: ctx as *mut c_void,
        send_cq: cq,
        recv_cq: cq,
        srq: ptr::null_mut(),
        cap: qp_cap,
        qp_type: ibv_qp_type::IBV_QPT_RC,
        sq_sig_all: 0, // will NOT submit work completion for all requests
    };
    let qp = unsafe { ibv_create_qp(pd, &mut qp_init_attr) };
    assert!(!qp.is_null());

    let attr_mask = ibv_qp_attr_mask::IBV_QP_STATE
        | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
        | ibv_qp_attr_mask::IBV_QP_PORT
        | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
    let qp_access_flags = ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;

    let qp_attr = MaybeUninit::<ibv_qp_attr>::uninit();
    let mut qp_attr = unsafe { qp_attr.assume_init() };
    qp_attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
    qp_attr.qp_access_flags = qp_access_flags.0;
    qp_attr.pkey_index = 0;
    qp_attr.port_num = config.ib_port;

    let err = unsafe { ibv_modify_qp(qp, &mut qp_attr, attr_mask.0 as i32) };
    assert_eq!(err, 0);

    qp
}

/// modify state of a queue pair to READY-TO-RECIEVE
/// - `lid`: dest qp lid
/// - `psn`: recieve queue psn
/// - `qpn`: dest qp number
pub unsafe fn modify_qp_rtr(qp: *mut ibv_qp, lid: u16, psn: u32, qpn: u32, config: &Config) {
    let attr_mask = ibv_qp_attr_mask::IBV_QP_STATE
        | ibv_qp_attr_mask::IBV_QP_AV
        | ibv_qp_attr_mask::IBV_QP_PATH_MTU
        | ibv_qp_attr_mask::IBV_QP_DEST_QPN
        | ibv_qp_attr_mask::IBV_QP_RQ_PSN
        | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
        | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;

    let mut qp_attr: ibv_qp_attr = unsafe { std::mem::zeroed() };
    qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
    qp_attr.path_mtu = config.qp_mtu;
    qp_attr.rq_psn = psn;
    qp_attr.dest_qp_num = qpn;
    qp_attr.ah_attr.dlid = lid;
    qp_attr.ah_attr.sl = 0; // service level
    qp_attr.ah_attr.src_path_bits = 0;
    qp_attr.ah_attr.is_global = 0;
    qp_attr.ah_attr.port_num = config.ib_port;
    qp_attr.max_dest_rd_atomic = config.qp_max_rd_atomic;
    qp_attr.min_rnr_timer = config.qp_min_rnr_timer;

    let err = unsafe { ibv_modify_qp(qp, &mut qp_attr, attr_mask.0 as i32) };
    assert_eq!(err, 0);
}

/// modify state of a queue pair to READY-TO-SEND
/// - `lid`: dest qp lid
/// - `psn`: send queue psn
/// - `qpn`: dest qp number
pub unsafe fn modify_qp_rts(qp: *mut ibv_qp, psn: u32, config: &Config) {
    let attr_mask = ibv_qp_attr_mask::IBV_QP_STATE
        | ibv_qp_attr_mask::IBV_QP_SQ_PSN
        | ibv_qp_attr_mask::IBV_QP_TIMEOUT
        | ibv_qp_attr_mask::IBV_QP_RETRY_CNT
        | ibv_qp_attr_mask::IBV_QP_RNR_RETRY
        | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;

    let qp_attr = MaybeUninit::<ibv_qp_attr>::uninit();
    let mut qp_attr: ibv_qp_attr = unsafe { qp_attr.assume_init() };
    qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
    qp_attr.sq_psn = psn;
    qp_attr.max_rd_atomic = config.qp_max_rd_atomic;
    qp_attr.timeout = config.qp_timeout;
    qp_attr.retry_cnt = config.qp_retry_cnt;
    qp_attr.rnr_retry = config.qp_rnr_retry;

    let err = unsafe { ibv_modify_qp(qp, &mut qp_attr, attr_mask.0 as i32) };
    assert_eq!(err, 0);
}

#[inline(always)]
unsafe fn post_send(
    opcode: ibv_wr_opcode,
    send_flags: ibv_send_flags,
    qp: *mut ibv_qp,
    lkey: u32,
    rkey: u32,
    remote_addr: u64,
    local_addr: u64,
    length: u32,
    wr_id: u64,
) -> bool {
    let mut sge = ibv_sge {
        addr: local_addr,
        length: length,
        lkey: lkey,
    };
    let mut wr: MaybeUninit<_> = MaybeUninit::<ibv_send_wr>::uninit();
    unsafe {
        let wr = wr.assume_init_mut();
        wr.wr_id = wr_id;
        wr.next = ptr::null_mut();
        wr.sg_list = &mut sge;
        wr.num_sge = 1;
        wr.opcode = opcode;
        wr.send_flags = send_flags.0;
        wr.wr.rdma.remote_addr = remote_addr;
        wr.wr.rdma.rkey = rkey;
    }
    let mut bad_wr = ptr::null_mut();
    let err = unsafe {
        let ctx = (*qp).context;
        let post_send = &(*ctx).ops.post_send.as_mut().unwrap_unchecked();
        post_send(qp, wr.as_mut_ptr(), &mut bad_wr)
    };
    match err {
        0 => true,
        nix::libc::ENOMEM => false,
        nix::libc::EINVAL => panic!("ibv_post_send: invalid value provided in wr"),
        nix::libc::EFAULT => panic!("ibv_post_send: invalid value provided in qp"),
        e => panic!("ibv_post_send: failed, err = {e}"),
    }
}

#[inline(always)]
pub unsafe fn post_read_signal(
    qp: *mut ibv_qp,
    lkey: u32,
    rkey: u32,
    remote_addr: u64,
    local_addr: u64,
    length: u32,
    wr_id: u64,
) -> bool {
    unsafe {
        post_send(
            ibv_wr_opcode::IBV_WR_RDMA_READ,
            ibv_send_flags::IBV_SEND_SIGNALED,
            qp,
            lkey,
            rkey,
            remote_addr,
            local_addr,
            length,
            wr_id,
        )
    }
}

#[inline(always)]
pub unsafe fn post_write_signal(
    qp: *mut ibv_qp,
    lkey: u32,
    rkey: u32,
    remote_addr: u64,
    local_addr: u64,
    length: u32,
    wr_id: u64,
) -> bool {
    unsafe {
        post_send(
            ibv_wr_opcode::IBV_WR_RDMA_WRITE,
            ibv_send_flags::IBV_SEND_SIGNALED,
            qp,
            lkey,
            rkey,
            remote_addr,
            local_addr,
            length,
            wr_id,
        )
    }
}

#[inline(always)]
pub unsafe fn ibv_poll_cq(cq: *mut ibv_cq, num_entries: i32, wc: *mut ibv_wc) -> i32 {
    unsafe {
        let ctx = (*cq).context;
        let poll_cq = &(*ctx).ops.poll_cq.as_mut().unwrap_unchecked();
        poll_cq(cq, num_entries, wc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        println!("Hello");

        unsafe {
            let mut num_devices: i32 = 0;
            let devices = ibv_get_device_list(&mut num_devices);
            ibv_free_device_list(devices);
            println!("num_devices: {}", num_devices);
        }
    }
}
