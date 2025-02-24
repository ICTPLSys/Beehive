#pragma once

#include "BackEndService.hpp"
#include "defs.hpp"
#include "request.hpp"
#include "test/fixed_size_string.hpp"

namespace social_network {

class SimpleBackEndServer {
public:
    SimpleBackEndServer();

    void compose_post(std::string_view username, int64_t user_id,
                      std::string_view text,
                      const LocalVector<int64_t> &media_ids,
                      const LocalVector<std::string> &media_types,
                      const PostType post_type,
                      Beehive::DereferenceScope &scope, ComposePostReq *req);

    LocalVector<Post> read_user_timeline(int64_t user_id, int start, int stop,
                                         Beehive::DereferenceScope &scope,
                                         ReadUserTimelineReq *req);

    LocalVector<Post> read_home_timeline(int64_t user_id, int start, int stop,
                                         Beehive::DereferenceScope &scope,
                                         ReadHomeTimelineReq *req);

    std::string login(std::string_view username, std::string_view password,
                      Beehive::DereferenceScope &scope);

    void register_user(std::string_view first_name, std::string_view last_name,
                       std::string_view username, std::string_view password,
                       Beehive::DereferenceScope &scope);

    void register_user_with_id(std::string_view first_name,
                               std::string_view last_name,
                               std::string_view username,
                               std::string_view password, const int64_t user_id,
                               Beehive::DereferenceScope &scope);

    LocalVector<int64_t> get_followers(int64_t user_id,
                                       Beehive::DereferenceScope &scope);

    void unfollow(int64_t user_id, int64_t followee_id,
                  Beehive::DereferenceScope &scope);

    void unfollow_with_username(std::string_view user_username,
                                std::string_view followee_username,
                                Beehive::DereferenceScope &scope);

    void follow(int64_t user_id, int64_t followee_id,
                Beehive::DereferenceScope &scope, FollowReq *req);

    void follow_with_username(std::string_view user_username,
                              std::string_view followee_username,
                              Beehive::DereferenceScope &scope);

    LocalVector<int64_t> get_followees(int64_t user_id,
                                       Beehive::DereferenceScope &scope);

    void upload_media(std::string_view filename, std::string_view data,
                      Beehive::DereferenceScope &scope);

    FixedSizeString<DataLen> get_media(std::string_view filename,
                                       Beehive::DereferenceScope &scope);

    void remove_posts(int64_t user_id, int start, int stop,
                      Beehive::DereferenceScope &scope);

    void NoOp();

    ServiceResult serve_request(Request *req) {
        // printf("serve request type %d\n", req->get_type());
        RootDereferenceScope scope;
        switch (req->get_type()) {
        case COMPOSE_POST: {
            auto *cpr = reinterpret_cast<ComposePostReq *>(req);
            compose_post(cpr->username, cpr->user_id, cpr->text, cpr->media_ids,
                         cpr->media_types, cpr->post_type, scope, cpr);
            break;
        }
        case READ_USERTIMELINE: {
            auto *rurt = reinterpret_cast<ReadUserTimelineReq *>(req);
            auto postList = read_user_timeline(rurt->user_id, rurt->start,
                                               rurt->stop, scope, rurt);
            return postList;
        }
        case LOGIN: {
            auto *lr = reinterpret_cast<LoginReq *>(req);
            return login(lr->username, lr->password, scope);
        }
        case REGISTER_USER: {
            auto *rur = reinterpret_cast<RegisterUserReq *>(req);
            register_user(rur->first_name, rur->last_name, rur->username,
                          rur->password, scope);
            break;
        }
        case REGISTER_USER_WITH_ID: {
            auto *rurwi = reinterpret_cast<RegisterUserWithIdReq *>(req);
            register_user_with_id(rurwi->first_name, rurwi->last_name,
                                  rurwi->username, rurwi->password,
                                  rurwi->user_id, scope);
            break;
        }
        case GET_FOLLOWERS: {
            auto *gfr = reinterpret_cast<GetFollowersReq *>(req);
            auto followers = get_followers(gfr->user_id, scope);
            return followers;
        }
        case UNFOLLOW: {
            auto *ur = reinterpret_cast<UnfollowReq *>(req);
            unfollow(ur->user_id, ur->followee_id, scope);
            break;
        }
        case UNFOLLOW_WITH_USERNAME: {
            auto *uur = reinterpret_cast<UnfollowWithUsernameReq *>(req);
            unfollow_with_username(uur->user_username, uur->followee_username,
                                   scope);
            break;
        }
        case FOLLOW: {
            auto *fr = reinterpret_cast<FollowReq *>(req);
            follow(fr->user_id, fr->followee_id, scope, fr);
            break;
        }
        case FOLLOW_WITH_USERNAME: {
            auto *fwr = reinterpret_cast<FollowWithUsernameReq *>(req);
            follow_with_username(fwr->user_username, fwr->followee_username,
                                 scope);
            break;
        }
        case GET_FOLLOWEES: {
            auto *gfer = reinterpret_cast<GetFolloweesReq *>(req);
            auto followees = get_followees(gfer->user_id, scope);
            return followees;
        }
        case READ_HOMETIMELINE: {
            auto *rht = reinterpret_cast<ReadHomeTimelineReq *>(req);
            auto postList = read_home_timeline(rht->user_id, rht->start,
                                               rht->stop, scope, rht);
            return postList;
        }
        case UPLOAD_MEDIA: {
            auto *umr = reinterpret_cast<UploadMediaReq *>(req);
            upload_media(umr->filename, umr->data, scope);
            break;
        }
        case GET_MEDIA: {
            auto *gmr = reinterpret_cast<GetMediaReq *>(req);
            auto media = get_media(gmr->filename, scope);
            return media.view();
        }
        case REMOVE_POSTS: {
            auto *rpr = reinterpret_cast<RemovePostsReq *>(req);
            remove_posts(rpr->user_id, rpr->start, rpr->stop, scope);
            break;
        }
        }
        return nullptr;
    }

    // void computeHometimelineSize();
    // void computeUsertimelineSize();
    // void computeFollowerSize();
    // void computeFolloweeSize();

private:
    BackEndService back_end_service_;
};

enum ErrorCode {
    SE_UNAUTHORIZED,
};

class ServiceException {
public:
    ErrorCode errorCode;
    std::string message;
};

}  // namespace social_network
