#pragma once

#include <ext/pb_ds/assoc_container.hpp>
#include <string_view>

// #include "OrderedVector.hpp"

#define HOSTNAME "http://short-url/"
// Custom Epoch (January 1, 2018 Midnight GMT = 2018-01-01T00:00:00Z)
#define CUSTOM_EPOCH 1514764800000
#include <ext/pb_ds/assoc_container.hpp>
#include <ext/pb_ds/tree_policy.hpp>

#include "config.hpp"
#include "data_structure/ConcurrentFlatHashmap.hpp"
#include "data_structure/concurrent_hashmap.hpp"
#include "data_structure/ordered_vector.hpp"
#include "test/fixed_size_string.hpp"

namespace social_network {
using namespace FarLib;

// FIXME: OrderedVector
using Timeline = OrderedVector<std::pair<int64_t, int64_t>>;

template <typename T>
using LocalVector = std::vector<T>;

constexpr size_t UserNameLen = 31;
constexpr size_t TextLen = 1500;
constexpr size_t UrlLen = 63;
constexpr size_t ShortenedUrlLen = 31;
constexpr size_t SaltLen = 31;
constexpr size_t PasswordHashedLen = 200;
constexpr size_t FileNameLen = 200;
constexpr size_t DataLen = 200;
constexpr size_t MediaTypeLen = 7;
constexpr size_t PureTextLen = 32;
using UserNameString = FixedSizeString<UserNameLen>;
using UrlString = FixedSizeString<UrlLen>;
using TextString = FixedSizeString<TextLen>;
using SaltString = FixedSizeString<SaltLen>;
using PasswordHashedString = FixedSizeString<PasswordHashedLen>;
using ShortenedUrlString = FixedSizeString<ShortenedUrlLen>;
using FileNameString = FixedSizeString<FileNameLen>;
using DataString = FixedSizeString<DataLen>;
using MediaTypeString = FixedSizeString<MediaTypeLen>;

struct UserProfile {
    int64_t user_id;
    UserNameString first_name;
    UserNameString last_name;
    SaltString salt;
    PasswordHashedString password_hashed;

    template <class Archive>
    void serialize(Archive &ar) {
        ar(user_id, first_name, last_name, salt, password_hashed);
    }
};

enum LoginErrorCode { OK, NOT_REGISTERED, WRONG_PASSWORD };

enum class PostType {
    TEXT,   // 文本
    IMAGE,  // 图片
    VIDEO,  // 视频
    POST
};

class Media {
public:
    Media() = default;
    Media(int64_t id, std::string_view type) : media_id(id), media_type(type) {}

    int64_t media_id;
    MediaTypeString media_type;
};

class UserMention {
public:
    UserMention() = default;
    UserMention(std::string_view name, int64_t id)
        : username(name), user_id(id) {}

    UserNameString username;
    int64_t user_id;
};

class Url {
public:
    Url() = default;
    Url(std::string_view expanded, std::string_view shortened)
        : expanded_url(expanded), shortened_url(shortened) {}

    UrlString expanded_url;
    ShortenedUrlString shortened_url;
};

class Creator {
public:
    Creator() = default;
    Creator(std::string_view name, int64_t id) : username(name), user_id(id) {}

    UserNameString username;
    int64_t user_id;
};

class Post {
public:
    Post() = default;
    int64_t timestamp;
    int64_t post_id;
    Creator creator;
    PostType post_type;
    TextString text;
    Media media[kMaxNumMediasPerText];
    Url urls[kMaxNumUrlsPerText];
    UserMention user_mentions[kMaxNumMentionsPerText];
    Post(const Post &that) = default;
    Post &operator=(const Post &that) = default;
    Post(Post &&) = default;
    Post &operator=(Post &&that) = default;
    ~Post() = default;

    Post(int64_t ts, int64_t id, std::vector<Media> &&media, Creator creator,
         PostType pt, std::string_view text, std::vector<Url> &&urls,
         std::vector<UserMention> &&user_mentions)
        : timestamp(ts),
          post_id(id),
          creator(creator),
          post_type(post_type),
          text(text) {
        assert(media.size() <= kMaxNumMediasPerText);
        assert(urls.size() <= kMaxNumUrlsPerText);
        assert(user_mentions.size() <= kMaxNumMentionsPerText);
        std::memmove(this->media, media.data(), sizeof(Media) * media.size());
        std::memmove(this->urls, urls.data(), sizeof(Url) * urls.size());
        std::memmove(this->user_mentions, user_mentions.data(),
                     sizeof(UserMention) * user_mentions.size());
    }
};

class TextServiceReturn {
public:
    std::vector<UserMention> user_mentions;
    std::string text;       // 处理后的文本
    std::vector<Url> urls;  // 相关的URLs
};

}  // namespace social_network
