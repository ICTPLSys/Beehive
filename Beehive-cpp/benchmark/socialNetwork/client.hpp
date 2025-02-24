#pragma once
#include <cstdint>
#include <optional>

#include "request.hpp"

struct SocialNetworkThreadState {
    SocialNetworkThreadState(size_t num_users,
                             std::optional<uint64_t> seed = std::nullopt)
        : gen(seed ? *seed : 0),
          dist_1_100(1, 100),
          dist_1_numusers(1, num_users),
          dist_0_charsetsize(0, std::size(kCharSet) - 2),
          dist_0_maxnummentions(0, kMaxNumMentionsPerText),
          dist_0_maxnumurls(0, kMaxNumUrlsPerText),
          dist_0_maxnummedias(0, kMaxNumMediasPerText),
          dist_0_maxint64(0, std::numeric_limits<int64_t>::max()) {}

    std::random_device rd;
    std::mt19937 gen;
    std::uniform_int_distribution<> dist_1_100;
    std::uniform_int_distribution<> dist_1_numusers;
    std::uniform_int_distribution<> dist_0_charsetsize;
    std::uniform_int_distribution<> dist_0_maxnummentions;
    std::uniform_int_distribution<> dist_0_maxnumurls;
    std::uniform_int_distribution<> dist_0_maxnummedias;
    std::uniform_int_distribution<int64_t> dist_0_maxint64;
};

std::string random_string(uint32_t len, SocialNetworkThreadState *state) {
    std::string str = "";
    for (uint32_t i = 0; i < len; i++) {
        str += kCharSet[(state->dist_0_charsetsize)(state->gen)];
    }
    return str;
}

struct config {
    uint64_t op_duration_ns;
    uint64_t op_speed_reset_ns;
    uint64_t max_runtime_ns;
    uint64_t max_request_count;
};
