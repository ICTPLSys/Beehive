#include "SimpleBackEndServer.hpp"

#include "defs.hpp"
#include "test/fixed_size_string.hpp"

namespace social_network {

SimpleBackEndServer::SimpleBackEndServer() : back_end_service_() {}

void SimpleBackEndServer::compose_post(
    std::string_view username, int64_t user_id, std::string_view text,
    const LocalVector<int64_t> &media_ids,
    const LocalVector<std::string> &media_types, const PostType post_type,
    Beehive::DereferenceScope &scope, ComposePostReq *req) {
    req->back_end_service_ = &back_end_service_;
    back_end_service_.ComposePost(username, user_id, text, media_ids,
                                  media_types, post_type, scope);
}

LocalVector<Post> SimpleBackEndServer::read_user_timeline(
    int64_t user_id, int start, int stop, Beehive::DereferenceScope &scope,
    ReadUserTimelineReq *req) {
    req->back_end_service_ = &back_end_service_;
    auto ret =
        back_end_service_.ReadUserTimeline(user_id, start, stop, scope, req);
    return ret;
}

LocalVector<Post> SimpleBackEndServer::read_home_timeline(
    int64_t user_id, int start, int stop, Beehive::DereferenceScope &scope,
    ReadHomeTimelineReq *req) {
    req->back_end_service_ = &back_end_service_;
    return back_end_service_.ReadHomeTimeline(user_id, start, stop, scope, req);
}

std::string SimpleBackEndServer::login(std::string_view username,
                                       std::string_view password,
                                       Beehive::DereferenceScope &scope) {
    auto variant = back_end_service_.Login(username, password, scope);
    if (std::holds_alternative<LoginErrorCode>(variant)) {
        ServiceException se;
        se.errorCode = ErrorCode::SE_UNAUTHORIZED;
        auto &login_error_code = std::get<LoginErrorCode>(variant);
        switch (login_error_code) {
        case NOT_REGISTERED:
            se.message = "The username is not registered yet.";
            break;
        case WRONG_PASSWORD:
            se.message = "Wrong password.";
            break;
        default:
            break;
        }
        throw se;
    }
    auto ret = std::get<std::string>(variant);
    return ret;
}

void SimpleBackEndServer::register_user(std::string_view first_name,
                                        std::string_view last_name,
                                        std::string_view username,
                                        std::string_view password,
                                        Beehive::DereferenceScope &scope) {
    back_end_service_.RegisterUser(first_name, last_name, username, password,
                                   scope);
}

void SimpleBackEndServer::register_user_with_id(
    std::string_view first_name, std::string_view last_name,
    std::string_view username, std::string_view password, const int64_t user_id,
    Beehive::DereferenceScope &scope) {
    back_end_service_.RegisterUserWithId(first_name, last_name, username,
                                         password, user_id, scope);
}

LocalVector<int64_t> SimpleBackEndServer::get_followers(
    int64_t user_id, Beehive::DereferenceScope &scope) {
    return back_end_service_.GetFollowers(user_id, scope);
}

void SimpleBackEndServer::unfollow(int64_t user_id, int64_t followee_id,
                                   Beehive::DereferenceScope &scope) {
    back_end_service_.Unfollow(user_id, followee_id, scope);
}

void SimpleBackEndServer::unfollow_with_username(
    std::string_view user_username, std::string_view followee_username,
    Beehive::DereferenceScope &scope) {
    back_end_service_.UnfollowWithUsername(user_username, followee_username,
                                           scope);
}

void SimpleBackEndServer::follow(int64_t user_id, int64_t followee_id,
                                 Beehive::DereferenceScope &scope,
                                 FollowReq *req) {
    req->back_end_service_ = &back_end_service_;
    back_end_service_.Follow(user_id, followee_id, scope, req);
}

void SimpleBackEndServer::follow_with_username(
    std::string_view user_username, std::string_view followee_username,
    Beehive::DereferenceScope &scope) {
    back_end_service_.FollowWithUsername(user_username, followee_username,
                                         scope);
}

LocalVector<int64_t> SimpleBackEndServer::get_followees(
    int64_t user_id, Beehive::DereferenceScope &scope) {
    return back_end_service_.GetFollowees(user_id, scope);
}

void SimpleBackEndServer::upload_media(std::string_view filename,
                                       std::string_view data,
                                       Beehive::DereferenceScope &scope) {
    back_end_service_.UploadMedia(filename, data, scope);
}

FixedSizeString<DataLen> SimpleBackEndServer::get_media(
    std::string_view filename, Beehive::DereferenceScope &scope) {
    return back_end_service_.GetMedia(filename, scope);
}

void SimpleBackEndServer::remove_posts(int64_t user_id, int start, int stop,
                                       Beehive::DereferenceScope &scope) {
    back_end_service_.RemovePosts(user_id, start, stop, scope);
}

// void SimpleBackEndServer::computeHometimelineSize() {
//   back_end_service_.computeHometimelineSize();
// }
// void SimpleBackEndServer::computeUsertimelineSize() {
//   back_end_service_.computeUsertimelineSize();
// }

// void SimpleBackEndServer::computeFollowerSize() {
//   back_end_service_.computeFollowerSize();
// }
// void SimpleBackEndServer::computeFolloweeSize() {
//   back_end_service_.computeFolloweeSize();
// }

void SimpleBackEndServer::NoOp() {}

}  // namespace social_network
