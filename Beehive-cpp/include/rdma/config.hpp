/**
 * RDMA Configuration
 */
#pragma once

#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include "utils/debug.hpp"

namespace FarLib {

// Basic parameters, not configurable
constexpr size_t PAGE_SIZE = 4096;

namespace rdma {

struct Configure {
#define CONFIG(TYPE, VAR, DEFAULT) TYPE VAR = DEFAULT;
#include "config.def"
#undef CONFIG
    static constexpr uint8_t MODE_ENABLE_CACHE = 0x01;
    static constexpr uint8_t MODE_ENABLE_UTHREAD = 0x02;
    void from_file(const char *filename) {
        std::ifstream ifs(filename);
        if (!ifs.is_open()) {
            std::cerr << "Error: can not open configuration file: " << filename
                      << std::endl;
            std::abort();
        }
        std::string name;
        while (ifs >> name) {
            if (name.size() > 0 && name[0] == '#') {
                while (true) {
                    int ch = ifs.get();
                    if (ch == '\n' || ch == EOF) break;
                }
                continue;
            }
#define CONFIG(TYPE, VAR, DEFAULT)                                      \
    if (name == #VAR) {                                                 \
        if (!(ifs >> this->VAR)) {                                      \
            std::cerr << "Error when reading configuration of " << name \
                      << ". Expected type is " << #TYPE << std::endl;   \
            std::abort();                                               \
        }                                                               \
        continue;                                                       \
    }
#include "config.def"
#undef CONFIG
            std::cerr << "Unknown configuration name: " << name << std::endl;
            std::abort();
        }
    }

    void self_check() const {
        std::cerr << max_thread_cnt * qp_count * qp_send_cap << std::endl;
        ASSERT(cq_entries >= max_thread_cnt * qp_count * qp_send_cap);
        // ASSERT(server_buffer_size >= client_buffer_size);
        ASSERT(PAGE_SIZE <= this->server_buffer_size);
        ASSERT(this->check_cq_batch_size <= this->cq_entries);
    }
};

}  // namespace rdma
}  // namespace FarLib