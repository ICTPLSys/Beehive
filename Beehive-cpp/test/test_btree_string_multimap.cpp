#include <chrono>
#include <fstream>
#include <string_view>
#include <thread>

#include "data_structure/btree_string_multimap.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

using namespace FarLib;
using namespace FarLib::rdma;
using namespace std::chrono_literals;

Configure config;

void test_kvpairs(std::string_view key, size_t n) {
    std::cout << "testing key = " << key << "; n = " << n << std::endl;
    uint32_t hash = std::hash<std::string_view>{}(key);
    RootDereferenceScope scope;
    KVPairs<int> pairs(key, hash, scope);
    // allocation
    for (size_t i = 0; i < n; i++) {
        pairs.append(i, scope);
    }
    size_t expected_i = 0;
    pairs.for_each_value(
        [&](size_t i, int x) {
            ASSERT(i == expected_i);
            ASSERT(x == i);
            expected_i++;
        },
        scope);
    ASSERT(expected_i == n);
}

void test_leaf() {
    using Leaf = BTreeMultiMap<int>::Leaf;
    RootDereferenceScope scope;
    Leaf leaf;
    const char *keys[] = {"c", "a", "e", "b", "h", "g", "d", "f"};
    for (size_t i = 0; i < 8; i++) {
        ASSERT(leaf.size == i);
        for (size_t j = 0; j < i; j++) {
            ASSERT(!leaf.insert(keys[j], 1, 0, scope));
        }
        ASSERT(leaf.size == i);
        ASSERT(leaf.insert(keys[i], 1, 0, scope));
    }
}

void print_btree(BTreeMultiMap<int> &tree, DereferenceScope &scope) {
    for (auto it = tree.begin(); it != tree.end(); it++) {
        auto accessor = it->get_head_chunk().access(scope);
        auto head = static_cast<const KVPairs<int>::Head *>(accessor.as_ptr());
        std::cout << std::setw(8) << head->get_key();
        it->for_each_value(
            [&](size_t, int v) { std::cout << std::setw(4) << v; }, scope);
        std::cout << std::endl;
    }
    std::cout << std::endl;
}

void test_btree() {
    RootDereferenceScope scope;
    BTreeMultiMap<int> tree;
    tree.insert("fuck", 1, 0, scope);
    tree.insert("you", 2, 0, scope);
    tree.insert("fuck", 3, 0, scope);
    print_btree(tree, scope);

    const char *slist[] = {"aa",     "bb",    "cc",      "wtf",   "bbb",
                           "stupid", "silly", "aax",     "bbx",   "ccx",
                           "wtfx",   "bbbx",  "stupidx", "sillyx"};
    for (size_t i = 0; i < 14; i++) {
        for (size_t j = 0; j < i; j++) {
            tree.insert(slist[i], j, 0, scope);
        }
    }
    print_btree(tree, scope);
}

void test_btree(const char *input_file) {
    std::ifstream ifs(input_file);
    RootDereferenceScope scope;
    BTreeMultiMap<int> tree;
    std::string s;
    size_t count = 0;
    while (ifs >> s) {
        if (++count % 1'000'000 == 0) std::cout << count << std::endl;
        tree.insert(s, 1, 0, scope);
    }
    std::cout << " map size: " << tree.size() << std::endl;
    ifs.close();
    tree.clear();
}

void test() {
    test_kvpairs("test", 1);
    test_kvpairs("test", 3);
    test_kvpairs("very-very-very-long-string", 16);
    test_kvpairs("test", 666);
    test_kvpairs("qwertyuiopasdfghjklzcxvbnm", 1000);
    test_kvpairs("test", 1018);
    test_kvpairs("test", 1019);
    test_kvpairs("test", 1024);
    test_kvpairs("test", 1234);
    test_kvpairs("test", 8765);
    test_kvpairs("test", 64 * 1024);
    test_kvpairs("test", 1024 * 1024);
    test_kvpairs("test", 64 * 1024 * 1024);
    test_leaf();
    test_btree();
    test_btree(
        "/path/to/data/word_count/training-monolingual/"
        "news.2009.es.shuffled");
}

int main() {
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.server_buffer_size = 1024L * 1024 * 1024 * 2;
    config.client_buffer_size = 64 * 1024 * 1024;
    config.evict_batch_size = 64 * 1024;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    FarLib::runtime_init(config);
    test();
    FarLib::runtime_destroy();
    server_thread.join();
    return 0;
}
