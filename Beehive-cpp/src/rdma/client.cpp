#include "rdma/client.hpp"

namespace Beehive {
namespace rdma {

std::unique_ptr<ClientControl> ClientControl::default_instance;
thread_local Client client;
}  // namespace rdma
}  // namespace Beehive