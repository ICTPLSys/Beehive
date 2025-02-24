#pragma once

#include <iostream>

#include "defs.hpp"

namespace social_network {
class BackEndService;

enum RequestType {
    COMPOSE_POST,
    READ_USERTIMELINE,
    LOGIN,
    REGISTER_USER,
    REGISTER_USER_WITH_ID,
    GET_FOLLOWERS,
    UNFOLLOW,
    UNFOLLOW_WITH_USERNAME,
    FOLLOW,
    FOLLOW_WITH_USERNAME,
    GET_FOLLOWEES,
    READ_HOMETIMELINE,
    UPLOAD_MEDIA,
    GET_MEDIA,
    REMOVE_POSTS
};

class Request {
public:
    virtual RequestType get_type() const = 0;
    virtual ~Request() = default;
    uint64_t request_start_ns;
};

struct ComposePostReq : public Request {
    std::string username;
    int64_t user_id;
    std::string text;
    LocalVector<int64_t> media_ids;
    LocalVector<std::string> media_types;
    PostType post_type;
    LocalVector<Url> target_urls;
    LocalVector<UserMention> user_mentions;
    BackEndService *back_end_service_;
    std::string updated_text;

    RequestType get_type() const override { return COMPOSE_POST; }
    ComposePostReq(const ComposePostReq &other)
        : username(other.username),
          user_id(other.user_id),
          text(other.text),
          media_ids(other.media_ids),
          media_types(other.media_types),
          post_type(other.post_type) {}

    ComposePostReq(const std::string &uname, int64_t uid,
                   const std::string &txt, const LocalVector<int64_t> &m_ids,
                   const LocalVector<std::string> &m_types, PostType p_type)
        : username(uname),
          user_id(uid),
          text(txt),
          media_ids(m_ids),
          media_types(m_types),
          post_type(p_type) {}

    ComposePostReq() = default;
    virtual ~ComposePostReq() = default;
};

struct ReadUserTimelineReq : public Request {
    int64_t user_id;
    int start;
    int stop;
    LocalVector<Post> posts;
    BackEndService *back_end_service_;

    RequestType get_type() const override { return READ_USERTIMELINE; }

    ReadUserTimelineReq(const ReadUserTimelineReq &other)
        : user_id(other.user_id), start(other.start), stop(other.stop) {}

    ReadUserTimelineReq(int64_t user_id_, int start_, int stop_)
        : user_id(user_id_), start(start_), stop(stop_) {}

    ReadUserTimelineReq() {}
    virtual ~ReadUserTimelineReq() = default;
};

struct LoginReq : public Request {
    std::string username;
    std::string password;

    RequestType get_type() const override { return LOGIN; }

    LoginReq(const LoginReq &other)
        : username(other.username), password(other.password) {}

    LoginReq(const std::string &username_, const std::string &password_)
        : username(username_), password(password_) {}
    virtual ~LoginReq() = default;
};

struct RegisterUserReq : public Request {
    std::string first_name;
    std::string last_name;
    std::string username;
    std::string password;

    RequestType get_type() const override { return REGISTER_USER; }

    RegisterUserReq(const RegisterUserReq &other)
        : first_name(other.first_name),
          last_name(other.last_name),
          username(other.username),
          password(other.password) {}

    RegisterUserReq(const std::string &first_name_,
                    const std::string &last_name_, const std::string &username_,
                    const std::string &password_)
        : first_name(first_name_),
          last_name(last_name_),
          username(username_),
          password(password_) {}
    virtual ~RegisterUserReq() = default;
};

struct RegisterUserWithIdReq : public Request {
    std::string first_name;
    std::string last_name;
    std::string username;
    std::string password;
    int64_t user_id;

    RequestType get_type() const override { return REGISTER_USER_WITH_ID; }

    // 带参数的构造函数
    RegisterUserWithIdReq(const std::string &fname, const std::string &lname,
                          const std::string &uname, const std::string &passwd,
                          int64_t uid)
        : first_name(fname),
          last_name(lname),
          username(uname),
          password(passwd),
          user_id(uid) {}

    RegisterUserWithIdReq(const RegisterUserWithIdReq &other)
        : first_name(other.first_name),
          last_name(other.last_name),
          username(other.username),
          password(other.password),
          user_id(other.user_id) {}
    virtual ~RegisterUserWithIdReq() = default;
};

struct GetFollowersReq : public Request {
    int64_t user_id;

    RequestType get_type() const override { return GET_FOLLOWERS; }

    GetFollowersReq(const GetFollowersReq &other) : user_id(other.user_id) {}

    GetFollowersReq(int64_t user_id_) : user_id(user_id_) {}

    virtual ~GetFollowersReq() = default;
};

struct UnfollowReq : public Request {
    int64_t user_id;
    int64_t followee_id;

    RequestType get_type() const override { return UNFOLLOW; }

    UnfollowReq(const UnfollowReq &other)
        : user_id(other.user_id), followee_id(other.followee_id) {}

    UnfollowReq(int64_t user_id_, int64_t followee_id_)
        : user_id(user_id_), followee_id(followee_id_) {}
    virtual ~UnfollowReq() = default;
};

struct UnfollowWithUsernameReq : public Request {
    std::string user_username;
    std::string followee_username;

    RequestType get_type() const override { return UNFOLLOW_WITH_USERNAME; }

    UnfollowWithUsernameReq(const UnfollowWithUsernameReq &other)
        : user_username(other.user_username),
          followee_username(other.followee_username) {}

    UnfollowWithUsernameReq(const std::string &user_username_,
                            const std::string &followee_username_)
        : user_username(user_username_),
          followee_username(followee_username_) {}
    virtual ~UnfollowWithUsernameReq() = default;
};

struct FollowReq : public Request {
    int64_t user_id;
    int64_t followee_id;
    BackEndService *back_end_service_;

    RequestType get_type() const override { return FOLLOW; }

    FollowReq(const FollowReq &other)
        : user_id(other.user_id), followee_id(other.followee_id) {}

    FollowReq(int64_t user_id_, int64_t followee_id_)
        : user_id(user_id_), followee_id(followee_id_) {}

    FollowReq() = default;
    virtual ~FollowReq() = default;
};

struct FollowWithUsernameReq : public Request {
    std::string user_username;
    std::string followee_username;

    RequestType get_type() const override { return FOLLOW_WITH_USERNAME; }

    FollowWithUsernameReq(const FollowWithUsernameReq &other)
        : user_username(other.user_username),
          followee_username(other.followee_username) {}

    FollowWithUsernameReq(const std::string &user_username_,
                          const std::string &followee_username_)
        : user_username(user_username_),
          followee_username(followee_username_) {}
    virtual ~FollowWithUsernameReq() = default;
};

struct GetFolloweesReq : public Request {
    int64_t user_id;

    RequestType get_type() const override { return GET_FOLLOWEES; }

    GetFolloweesReq(const GetFolloweesReq &other) : user_id(other.user_id) {}

    GetFolloweesReq(int64_t user_id_) : user_id(user_id_) {}

    virtual ~GetFolloweesReq() = default;
};

struct ReadHomeTimelineReq : public Request {
    int64_t user_id;
    int start;
    int stop;
    LocalVector<Post> posts;
    BackEndService *back_end_service_;

    RequestType get_type() const override { return READ_HOMETIMELINE; }

    ReadHomeTimelineReq(const ReadHomeTimelineReq &other)
        : user_id(other.user_id), start(other.start), stop(other.stop) {}

    ReadHomeTimelineReq(int64_t user_id_, int start_, int stop_)
        : user_id(user_id_), start(start_), stop(stop_) {}

    ReadHomeTimelineReq() = default;

    virtual ~ReadHomeTimelineReq() = default;
};

struct UploadMediaReq : public Request {
    std::string filename;
    std::string data;

    RequestType get_type() const override { return UPLOAD_MEDIA; }

    UploadMediaReq(const UploadMediaReq &other)
        : filename(other.filename), data(other.data) {}

    UploadMediaReq(const std::string &filename_, const std::string &data_)
        : filename(filename_), data(data_) {}

    virtual ~UploadMediaReq() = default;
};

struct GetMediaReq : public Request {
    std::string filename;

    RequestType get_type() const override { return GET_MEDIA; }

    GetMediaReq(const GetMediaReq &other) : filename(other.filename) {}

    GetMediaReq(const std::string &filename_) : filename(filename_) {}

    virtual ~GetMediaReq() = default;
};

struct RemovePostsReq : public Request {
    int64_t user_id;
    int start;
    int stop;

    RequestType get_type() const override { return REMOVE_POSTS; }

    RemovePostsReq(const RemovePostsReq &other)
        : user_id(other.user_id), start(other.start), stop(other.stop) {}

    RemovePostsReq(int64_t user_id_, int start_, int stop_)
        : user_id(user_id_), start(start_), stop(stop_) {}

    RemovePostsReq() = default;
    virtual ~RemovePostsReq() = default;
};

using ServiceResult =
    std::variant<void *,                // 用于没有返回值的操作
                 LocalVector<int64_t>,  // 例如，GetFollowers的返回类型
                 std::string_view,      // 例如，Login的返回类型
                 LocalVector<Post>      // 例如，ReadUserTimeline的返回类型
                 >;

}  // namespace social_network