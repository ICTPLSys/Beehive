#pragma once

// #include <cereal/types/string.hpp>
// #include <nu/dis_hash_table.hpp>
// #include <nu/utils/farmhash.hpp>

#include <set>
#include <string>
#include <unordered_map>

#include "config.hpp"
#include "data_structure/concurrent_hashmap.hpp"
#include "data_structure/small_vector.hpp"
#include "defs.hpp"
#include "test/fixed_size_string.hpp"
#include "utils.hpp"

namespace social_network {
using str_key_t = Beehive::FixedSizeString<16>;
using str_value_t = Beehive::FixedSizeString<64>;
class States {
private:
    static std::unique_ptr<States> default_instance;

public:
    static States& getInstance() {
        if (default_instance.get() == nullptr) [[unlikely]] {
            default_instance.reset(new States());
        }
        return *default_instance.get();
    }

    static void destroyInstance() { default_instance.reset(); }

    States(States const&) = delete;
    States& operator=(States const&) = delete;

    constexpr static uint32_t kHashTablePowerNumShards = 9;

    template <class Archive>
    void serialize(Archive& ar) {
        ar(username_to_userprofile_map, filename_to_data_map,
           short_to_extended_map, userid_to_hometimeline_map,
           userid_to_usertimeline_map, postid_to_post_map,
           userid_to_followers_map, userid_to_followees_map, secret);
    }
    Beehive::ConcurrentHashMap<FixedSizeString<UserNameLen>, UserProfile>
        username_to_userprofile_map{NumEntriesShift};
    Beehive::ConcurrentHashMap<FixedSizeString<FileNameLen>,
                               FixedSizeString<DataLen>>
        filename_to_data_map{NumEntriesShift};
    Beehive::ConcurrentHashMap<FixedSizeString<ShortenedUrlLen>,
                               FixedSizeString<UrlLen>>
        short_to_extended_map{NumEntriesShift};
    Beehive::ConcurrentHashMap<int64_t, Post> postid_to_post_map{
        NumEntriesShift};

    ConcurrentFlatMap<int64_t, Timeline> userid_to_hometimeline_map;
    ConcurrentFlatMap<int64_t, Timeline> userid_to_usertimeline_map;
    ConcurrentFlatMap<int64_t, FarSmallVector<int64_t>> userid_to_followers_map;
    ConcurrentFlatMap<int64_t, FarSmallVector<int64_t>> userid_to_followees_map;
    std::string secret;

protected:
    States()
        : userid_to_hometimeline_map(NumEntriesShift),
          userid_to_usertimeline_map(NumEntriesShift),
          userid_to_followers_map(NumEntriesShift),
          userid_to_followees_map(NumEntriesShift) {
        secret = "secret";
    }  // 构造函数为 protected 确保外部不能创建实例
};

}  // namespace social_network
