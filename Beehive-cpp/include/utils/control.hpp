#pragma once
#include "rdma/config.hpp"

namespace Beehive {
void runtime_init(const rdma::Configure& config, bool enable_cache = true);
void runtime_destroy();
const rdma::Configure& get_config();
}  // namespace Beehive