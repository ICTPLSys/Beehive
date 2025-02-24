#include <infiniband/verbs.h>

struct ibv_context *open_ib_device();
struct ibv_mr *register_mr(struct ibv_pd *pd, void *addr, size_t length);
uint16_t query_lid(struct ibv_context *context, uint8_t port_num);
