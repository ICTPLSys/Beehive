#include "wrapper.h"

#include <infiniband/verbs.h>

/*
    open the first ib device
    TODO: select device by name
*/
struct ibv_context *open_ib_device() {
    int num_devices;
    struct ibv_device **devices;
    struct ibv_device *device;

    devices = ibv_get_device_list(&num_devices);
    if (devices == NULL || num_devices == 0) return NULL;
    device = devices[0];
    ibv_free_device_list(devices);
    return ibv_open_device(device);
}

struct ibv_mr *register_mr(struct ibv_pd *pd, void *addr, size_t length) {
    enum ibv_access_flags access_flags = IBV_ACCESS_LOCAL_WRITE |
                                         IBV_ACCESS_REMOTE_READ |
                                         IBV_ACCESS_REMOTE_WRITE;
    return ibv_reg_mr(pd, addr, length, access_flags);
}

uint16_t query_lid(struct ibv_context *context, uint8_t port_num) {
    struct ibv_port_attr port_attr;
    ibv_query_port(context, port_num, &port_attr);
    return port_attr.lid;
}
