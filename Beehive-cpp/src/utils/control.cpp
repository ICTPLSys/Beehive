#include "utils/control.hpp"

#include "cache/cache.hpp"
#include "rdma/config.hpp"
#include "utils/uthreads.hpp"

#ifndef NO_REMOTE

#include "rdma/client.hpp"

namespace FarLib {
using namespace FarLib::rdma;
static uint8_t mode;
static Configure global_config;
void runtime_init(const rdma::Configure &config, bool enable_cache) {
    mode = 0;
    config.self_check();
    global_config = config;
    if (enable_cache) {
        mode |= Configure::MODE_ENABLE_CACHE | Configure::MODE_ENABLE_UTHREAD;
    }
    ClientControl::init_default(config);
    client.init();
    if (mode & Configure::MODE_ENABLE_UTHREAD) {
        uthread::runtime_init(config.max_thread_cnt);
    }
    if (mode & Configure::MODE_ENABLE_CACHE) {
        Cache::init_default(Client::get_default()->get_buffer(),
                            config.client_buffer_size,
                            config.server_buffer_size, config.evict_batch_size);
    }
}

void runtime_destroy() {
    if (mode & Configure::MODE_ENABLE_CACHE) {
        Cache::destroy_default();
    }
    if (mode & Configure::MODE_ENABLE_UTHREAD) {
        uthread::runtime_destroy();
    }
    ClientControl::destroy_default();
}

const Configure &get_config() { return global_config; }
}  // namespace FarLib

#else

namespace FarLib {
using namespace FarLib::rdma;
static Configure global_config;
void runtime_init(const rdma::Configure &config, bool enable_cache) {
    global_config = config;
    uthread::runtime_init(config.max_thread_cnt);
#ifdef USE_BUMP_ALLOCATOR
    void *heap_ptr = mmap(
        nullptr, allocator::HeapSize, PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANON | MAP_HUGETLB | (21 << MAP_HUGE_SHIFT), -1, 0);
    if (heap_ptr == MAP_FAILED) {
        ERROR("mmap failed");
    }
    allocator::heap = static_cast<std::byte *>(heap_ptr);
#endif
}

void runtime_destroy() {
#ifdef USE_BUMP_ALLOCATOR
    uthread::runtime_destroy();
    munmap(allocator::heap, allocator::HeapSize);
#endif
}

const Configure &get_config() { return global_config; }
}  // namespace FarLib

#endif