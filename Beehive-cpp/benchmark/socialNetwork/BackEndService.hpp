#pragma once

#include <optional>
#include <string>
#include <variant>

#include "async/generic_stream.hpp"
#include "defs.hpp"
#include "request.hpp"
#include "states.hpp"
#include "test/fixed_size_string.hpp"
#include "utils.hpp"

namespace social_network {

class BackEndService {
public:
    BackEndService();
    void ComposePost(std::string_view username, int64_t user_id,
                     std::string_view text,
                     const LocalVector<int64_t> &media_ids,
                     const LocalVector<std::string> &media_types,
                     const PostType post_type, FarLib::DereferenceScope &scope,
                     ComposePostReq *req);
    void ComposePost(std::string_view username, int64_t user_id,
                     std::string_view text,
                     const LocalVector<int64_t> &media_ids,
                     const LocalVector<std::string> &media_types,
                     const PostType post_type, FarLib::DereferenceScope &scope);
    LocalVector<Post> ReadUserTimeline(int64_t user_id, int start, int stop,
                                       FarLib::DereferenceScope &scope,
                                       ReadUserTimelineReq *req);
    LocalVector<Post> ReadUserTimeline(int64_t user_id, int start, int stop,
                                       FarLib::DereferenceScope &scope);
    std::variant<LoginErrorCode, std::string> Login(
        std::string_view username, std::string_view password,
        FarLib::DereferenceScope &scope);
    void RegisterUser(std::string_view first_name, std::string_view last_name,
                      std::string_view username, std::string_view password,
                      FarLib::DereferenceScope &scope);
    void RegisterUserWithId(std::string_view first_name,
                            std::string_view last_name,
                            std::string_view username,
                            std::string_view password, const int64_t user_id,
                            FarLib::DereferenceScope &scope);
    LocalVector<int64_t> GetFollowers(int64_t user_id,
                                      FarLib::DereferenceScope &scope);
    void Unfollow(int64_t user_id, int64_t followee_id,
                  FarLib::DereferenceScope &scope);
    void UnfollowWithUsername(std::string_view user_name,
                              std::string_view followee_name,
                              FarLib::DereferenceScope &scope);
    void Follow(int64_t user_id, int64_t followee_id,
                FarLib::DereferenceScope &scope, FollowReq *req);
    void FollowWithUsername(std::string_view user_name,
                            std::string_view followee_name,
                            FarLib::DereferenceScope &scope);
    LocalVector<int64_t> GetFollowees(int64_t user_id,
                                      FarLib::DereferenceScope &scope);
    LocalVector<Post> ReadHomeTimeline(int64_t user_id, int start, int stop,
                                       FarLib::DereferenceScope &scope,
                                       ReadHomeTimelineReq *req);
    void UploadMedia(std::string_view filename, std::string_view data,
                     FarLib::DereferenceScope &scope);
    FixedSizeString<DataLen> GetMedia(std::string_view filename,
                                      FarLib::DereferenceScope &scope);
    void RemovePosts(int64_t user_id, int start, int stop,
                     FarLib::DereferenceScope &scope);
    std::optional<int64_t> GetUserId(std::string_view username,
                                     FarLib::DereferenceScope &scope);
    // void computeHometimelineSize();
    // void computeUsertimelineSize();
    // void computeFollowerSize();
    // void computeFolloweeSize();
    // void computeRatioSize();
    // void computeUrlSize();
    template <bool Mut>
    struct ReadUserTimeLineGetCont {
        ReadUserTimelineReq *request;
        ReadUserTimeLineGetCont(ReadUserTimelineReq *r) : request(r) {}
        void operator()(LiteAccessor<Post, Mut> p) {
            p.check();
            if (!p.is_null()) request->posts.emplace_back(std::move(*p));
            p.check();
        }
    };

    struct ReadUserTimelineCont {
        ReadUserTimelineReq *request;
        FarLib::async::GenericStreamRunner<512> *runner;
        ReadUserTimelineCont(ReadUserTimelineReq *req,
                             FarLib::DereferenceScope &scope)
            : request(req) {}
        ReadUserTimelineCont(ReadUserTimelineCont &&) = default;
        void operator()(std::vector<std::pair<int64_t, int64_t>> &&result,
                        const DereferenceScope &scope) {
            for (auto iter = result.begin(); iter != result.end(); iter++) {
                auto post_id = iter->second;
                ReadUserTimeLineGetCont<false> cont(request);
                runner->async_run([&](void *ctx) {
                    new (ctx) ConcurrentHashMap<int64_t, Post>::GetContext<
                        false, ReadUserTimeLineGetCont<false>>(
                        &request->back_end_service_->states_.postid_to_post_map,
                        &post_id, std::move(cont));
                });
            }
            runner->await();
        }
    };

    template <bool Mut>
    struct ReadHomeTimeLineGetCont {
        ReadHomeTimelineReq *request;
        ReadHomeTimeLineGetCont(ReadHomeTimelineReq *r) : request(r) {}
        void operator()(LiteAccessor<Post, Mut> p) {
            p.check();
            if (!p.is_null()) request->posts.emplace_back(std::move(*p));
            p.check();
        }
    };

    struct ReadHomeTimelineCont {
        ReadHomeTimelineReq *request;
        FarLib::async::GenericStreamRunner<512> *runner;
        ReadHomeTimelineCont(ReadHomeTimelineReq *req) : request(req) {}
        void operator()(std::vector<std::pair<int64_t, int64_t>> &&result,
                        const DereferenceScope &scope) {
            for (auto iter = result.begin(); iter != result.end(); iter++) {
                auto post_id = iter->second;
                ReadHomeTimeLineGetCont<false> cont(request);
                runner->async_run([&](void *ctx) {
                    new (ctx) ConcurrentHashMap<int64_t, Post>::GetContext<
                        false, ReadHomeTimeLineGetCont<false>>(
                        &request->back_end_service_->states_.postid_to_post_map,
                        &post_id, std::move(cont));
                });
            }
            runner->await();
        }
    };

    struct BoolCont {
        bool *result;
        BoolCont(bool *r) : result(r) {}
        void operator()(bool v) { *result = v; }
    };

    template <bool Mut>
    struct ComposeFirstCont {
        bool *found;
        ComposePostReq *req;
        std::string_view username;
        ComposeFirstCont(bool *found) : found(found) {}
        void operator()(LiteAccessor<UserProfile, Mut> p) {
            p.check();
            if (p.is_null())
                *found = false;
            else
                *found = true;
            UserProfile *result;
            *result = *p;
            req->user_mentions.emplace_back(std::move(username),
                                            result->user_id);
            p.check();
        }
    };

    template <bool Mut>
    struct LoginCont {
        UserProfile *result;
        bool *found;
        LoginCont(UserProfile *r, bool *found) : result(r), found(found) {}
        void operator()(LiteAccessor<UserProfile, Mut> p) {
            p.check();
            if (p.is_null())
                *found = false;
            else
                *found = true;
            *result = *p;
            p.check();
        }
    };

    struct GetFollowsCont {
        std::vector<int64_t> *res;
        void operator()(std::vector<int64_t> &&result) { *res = result; }
    };

    struct FollowCont {
        FollowReq *req;
        FarLib::async::GenericStreamRunner<512> *runner;
        FollowCont(FollowReq *req) : req(req) {}
        FollowCont(FollowCont &&) = default;
        void operator()(bool result, DereferenceScope &scope) {
            req->back_end_service_->states_.userid_to_followers_map
                .insert_or_update(
                    req->followee_id,
                    [&](FarSmallVector<int64_t> *ptr) {
                        new (ptr) FarSmallVector<int64_t>(scope);
                    },
                    [&](const int64_t &k, FarSmallVector<int64_t> &v) {
                        runner->async_run([&](void *ctx) {
                            new (ctx)
                                FarSmallVector<int64_t>::InsertContext<true>(
                                    &v, req->user_id);
                        });
                        // v.insert(req->user_id, scope);
                    },
                    &scope);
            runner->await();
        }
    };

private:
    TextServiceReturn ComposeText(std::string_view text,
                                  FarLib::DereferenceScope &scope);
    LocalVector<UserMention> ComposeUserMentions(
        const LocalVector<std::string> &usernames,
        FarLib::DereferenceScope &scope);
    LocalVector<Url> ComposeUrls(const LocalVector<std::string> &urls,
                                 FarLib::DereferenceScope &scope);
    void WriteUserTimeline(int64_t post_id, int64_t user_id, int64_t timestamp,
                           FarLib::DereferenceScope &scope);
    void WriteHomeTimeline(int64_t post_id, int64_t user_id, int64_t timestamp,
                           const LocalVector<int64_t> &user_mentions_id,
                           FarLib::DereferenceScope &scope);
    LocalVector<Post> ReadPosts(const LocalVector<int64_t> &post_ids,
                                FarLib::DereferenceScope &scope);
    States &states_;
};
}  // namespace social_network
