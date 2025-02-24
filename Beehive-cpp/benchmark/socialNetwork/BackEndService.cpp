
#include "BackEndService.hpp"

#include <cstdint>
#include <iostream>
#include <string_view>

#include "defs.hpp"
#include "picosha2.h"
#include "test/fixed_size_string.hpp"

namespace social_network {
std::string generate_random_string_util(size_t length) {
    const std::string chars =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<> distribution(0, chars.size() - 1);

    std::string random_string;
    for (size_t i = 0; i < length; ++i) {
        random_string += chars[distribution(generator)];
    }

    return random_string;
}
static inline uint64_t rdtsc() {
    unsigned int lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
}

BackEndService::BackEndService() : states_(States::getInstance()) {}

// ComposeUrls: Shortens URLs found within the post text for display.
LocalVector<Url> BackEndService::ComposeUrls(
    const LocalVector<std::string> &urls, Beehive::DereferenceScope &scope) {
    LocalVector<Url> target_url_futures;

    for (ssize_t i = 0; i < urls.size(); i++) {
        Url target_url(urls[i], HOSTNAME + generate_random_string_util(10));
        states_.short_to_extended_map.put(target_url.shortened_url,
                                          target_url.expanded_url, scope);
        target_url_futures.emplace_back(target_url);
    }

    LocalVector<Url> target_urls;
    for (auto &target_url_future : target_url_futures) {
        target_urls.emplace_back(std::move(target_url_future));
    }
    return target_urls;
}

// ComposePost: Method for composing and submitting a post.
void BackEndService::ComposePost(std::string_view username, int64_t user_id,
                                 std::string_view text,
                                 const LocalVector<int64_t> &media_ids,
                                 const LocalVector<std::string> &media_types,
                                 const PostType post_type,
                                 Beehive::DereferenceScope &scope) {
    auto text_service_return = ComposeText(text, scope);
    // printf("%p compose text end\n", fibre_self());

    auto temp = std::chrono::high_resolution_clock::now().time_since_epoch();
    auto timestamp = rdtsc();
    auto unique_id = GenUniqueId();

    WriteUserTimeline(unique_id, user_id, timestamp, scope);
    // printf("%p write user timeline\n", fibre_self());

    LocalVector<Media> medias;
    // BUG_ON(media_types.size() != media_ids.size());
    for (int i = 0; i < media_ids.size(); ++i) {
        Media media;
        media.media_id = media_ids[i];
        media.media_type = media_types[i];
        medias.emplace_back(std::move(media));
    }

    // auto &text_service_return = text_service_return_future.get();
    LocalVector<int64_t> user_mention_ids;

    for (auto &item : text_service_return.user_mentions) {
        user_mention_ids.emplace_back(item.user_id);
    }

    WriteHomeTimeline(unique_id, user_id, timestamp, user_mention_ids, scope);
    // printf("%p write home timeline\n", fibre_self());

    Post post(timestamp, unique_id, std::move(medias),
              Creator(username, user_id), post_type, text_service_return.text,
              std::move(text_service_return.urls),
              std::move(text_service_return.user_mentions));

    // states_.postid_to_post_map.put(post.post_id, std::move(post));
    states_.postid_to_post_map.put(post.post_id, post, scope);
    // printf("%p put post\n", fibre_self());
}

// ComposePost: Method for composing and submitting a post.
void BackEndService::ComposePost(std::string_view username, int64_t user_id,
                                 std::string_view text,
                                 const LocalVector<int64_t> &media_ids,
                                 const LocalVector<std::string> &media_types,
                                 const PostType post_type,
                                 Beehive::DereferenceScope &scope,
                                 ComposePostReq *req) {
    // compose url
    LocalVector<std::string> urls = MatchUrls(text);
    Beehive::async::GenericStreamRunner<
        sizeof(
            ConcurrentHashMap<FixedSizeString<ShortenedUrlLen>,
                              FixedSizeString<UrlLen>>::PutContext<BoolCont>),
        true>
        runner(scope);
    for (ssize_t i = 0; i < urls.size(); i++) {
        Url target_url(urls[i], HOSTNAME + generate_random_string_util(10));
        req->target_urls.emplace_back(target_url);
        bool res;
        runner.async_run([&](async::GenericOrderedContext *ctx) {
            BoolCont cont(&res);
            new (ctx) ConcurrentHashMap<FixedSizeString<ShortenedUrlLen>,
                                        FixedSizeString<UrlLen>>::
                PutContext<BoolCont>(&states_.short_to_extended_map,
                                     target_url.shortened_url,
                                     target_url.expanded_url, std::move(cont));
        });
    }
    req->updated_text = ShortenUrlInText(req->text, req->target_urls);

    // compose user mention
    Beehive::async::GenericStreamRunner<sizeof(
        ConcurrentHashMap<FixedSizeString<UserNameLen>, UserProfile>::
            GetContext<false, ComposeFirstCont<false>>)>
        runner_mention(scope);
    bool found;
    LocalVector<std::string> usernames = MatchMentions(text);
    for (auto &username : usernames) {
        runner_mention.async_run([&](void *ctx) {
            ComposeFirstCont<false> cont(&found);
            cont.req = req;
            cont.username = username;
            FixedSizeString<UserNameLen> username_fix = cont.username;
            new (ctx)
                ConcurrentHashMap<FixedSizeString<UserNameLen>, UserProfile>::
                    GetContext<false, ComposeFirstCont<false>>(
                        &states_.username_to_userprofile_map, &username_fix,
                        std::move(cont));
        });
    }

    auto temp = std::chrono::high_resolution_clock::now().time_since_epoch();
    auto timestamp = rdtsc();
    auto unique_id = GenUniqueId();

    LocalVector<Media> medias;
    for (int i = 0; i < media_ids.size(); ++i) {
        Media media;
        media.media_id = media_ids[i];
        media.media_type = media_types[i];
        medias.emplace_back(std::move(media));
    }

    // auto &text_service_return = text_service_return_future.get();
    LocalVector<int64_t> user_mention_ids;

    for (auto &item : req->user_mentions) {
        user_mention_ids.emplace_back(item.user_id);
    }

    WriteUserTimeline(unique_id, user_id, timestamp, scope);

    WriteHomeTimeline(unique_id, user_id, timestamp, user_mention_ids, scope);

    Post post(timestamp, unique_id, std::move(medias),
              Creator(username, user_id), post_type, req->text,
              std::move(req->target_urls), std::move(req->user_mentions));

    // states_.postid_to_post_map.put(post.post_id, std::move(post));
    states_.postid_to_post_map.put(post.post_id, post, scope);
}

// ComposeText: Extracts and shortens URLs within the post text and identifies
// user mentions.
TextServiceReturn BackEndService::ComposeText(
    std::string_view text, Beehive::DereferenceScope &scope) {
    auto target_urls = ComposeUrls(MatchUrls(text), scope);
    auto updated_text = ShortenUrlInText(text, target_urls);
    TextServiceReturn text_service_return;
    text_service_return.user_mentions =
        std::move(ComposeUserMentions(MatchMentions(text), scope));
    text_service_return.text = std::move(updated_text);
    text_service_return.urls = std::move(target_urls);
    return text_service_return;
}

// ComposeUserMentions: Identifies and processes user mentions within a post.
LocalVector<UserMention> BackEndService::ComposeUserMentions(
    const LocalVector<std::string> &usernames,
    Beehive::DereferenceScope &scope) {
    LocalVector<UserMention> user_mentions;
    for (auto &username : usernames) {
        UserProfile user_profile;
        states_.username_to_userprofile_map.get(
            FixedSizeString<UserNameLen>(username), &user_profile, scope);
        user_mentions.emplace_back(std::move(username), user_profile.user_id);
    }

    return user_mentions;
}

// WriteUserTimeline: Updates the user's personal timeline with a new post.
void BackEndService::WriteUserTimeline(int64_t post_id, int64_t user_id,
                                       int64_t timestamp,
                                       Beehive::DereferenceScope &scope) {
    states_.userid_to_usertimeline_map.insert_or_update(
        user_id, [&](Timeline *ptr) { new (ptr) Timeline(scope); },
        [&](const int64_t &uid, Timeline &v) {
            v.insert(std::make_pair(timestamp, post_id), scope);
        },
        &scope);
}

// old
LocalVector<Post> BackEndService::ReadUserTimeline(
    int64_t user_id, int start, int stop, Beehive::DereferenceScope &scope) {
    if (stop <= start || start < 0) {
        return LocalVector<Post>();
    }

    LocalVector<int64_t> post_ids;
    states_.userid_to_usertimeline_map.get(
        user_id,
        [&](const int64_t &k, Timeline &v) {
            auto temp = v.get_range_elements(std::make_pair(start, 1L),
                                             std::make_pair(stop, 1L), scope);
            for (auto iter = temp.begin(); iter != temp.end(); iter++) {
                post_ids.push_back(iter->second);
            }
        },
        &scope);
    return ReadPosts(post_ids, scope);
}

// ReadUserTimeline: Retrieves posts from a user's personal timeline.
LocalVector<Post> BackEndService::ReadUserTimeline(
    int64_t user_id, int start, int stop, Beehive::DereferenceScope &scope,
    ReadUserTimelineReq *req) {
    if (stop <= start || start < 0) {
        return LocalVector<Post>();
    }

    Beehive::async::GenericStreamRunner<512> runner(scope);

    states_.userid_to_usertimeline_map.get(
        user_id,
        [&](const int64_t &k, Timeline &v) {
            ReadUserTimelineCont cont(req, scope);
            cont.runner = &runner;
            runner.async_run([&](void *ctx) {
                new (ctx) Timeline::GetContext<false, ReadUserTimelineCont>(
                    &v, std::make_pair(start, 1L), std::make_pair(stop, 1L),
                    std::move(cont));
            });
        },
        &scope);
    runner.await();
    return req->posts;
}

// ReadHomeTimeline: Retrieves posts from the home timeline, which includes
// followed users' posts.
LocalVector<Post> BackEndService::ReadHomeTimeline(
    int64_t user_id, int start, int stop, Beehive::DereferenceScope &scope,
    ReadHomeTimelineReq *req) {
    if (stop <= start || start < 0) {
        return LocalVector<Post>();
    }

    Beehive::async::GenericStreamRunner<512> runner(scope);

    states_.userid_to_hometimeline_map.get(
        user_id,
        [&](const int64_t &k, Timeline &v) {
            ReadHomeTimelineCont cont(req);
            cont.runner = &runner;
            runner.async_run([&](void *ctx) {
                new (ctx) Timeline::GetContext<false, ReadHomeTimelineCont>(
                    &v, std::make_pair(start, 1L), std::make_pair(stop, 1L),
                    std::move(cont));
            });
        },
        &scope);

    runner.await();
    return req->posts;
}

// RemovePosts: Deletes posts from the user's timeline and the timelines of
// followers and mentions.
void BackEndService::RemovePosts(int64_t user_id, int start, int stop,
                                 Beehive::DereferenceScope &scope) {
    ReadUserTimelineReq req;
    auto posts = std::move(ReadUserTimeline(user_id, start, stop, scope));
    auto followers = std::move(GetFollowers(user_id, scope));

    for (auto post : posts) {
        states_.postid_to_post_map.remove(post.post_id, scope);

        states_.userid_to_usertimeline_map.update<false>(
            user_id,
            [&](const int64_t &uid, Timeline &v) {
                v.erase(std::make_pair(post.timestamp, post.post_id), scope);
            },
            &scope);
        for (auto &mention : post.user_mentions) {
            states_.userid_to_hometimeline_map.update<false>(
                mention.user_id,
                [&](const int64_t &k, Timeline &v) {
                    v.erase(std::make_pair(post.timestamp, post.post_id),
                            scope);
                },
                &scope);
        }
        for (auto user_id : followers) {
            states_.userid_to_hometimeline_map.update<false>(
                user_id,
                [&](const int64_t &k, Timeline &v) {
                    v.erase(std::make_pair(post.timestamp, post.post_id),
                            scope);
                },
                &scope);
        }

        for (auto &url : post.urls) {
            states_.short_to_extended_map.remove(url.shortened_url, scope);
        }
    }
}
// WriteHomeTimeline: Updates the home timeline of followers and mentioned users
// with a new post.
void BackEndService::WriteHomeTimeline(
    int64_t post_id, int64_t user_id, int64_t timestamp,
    const LocalVector<int64_t> &user_mentions_id,
    Beehive::DereferenceScope &scope) {
    auto follower_ids = GetFollowers(user_id, scope);
    // printf("%p get follower\n", fibre_self());
    for (auto id : user_mentions_id) {
        states_.userid_to_hometimeline_map.insert_or_update(
            id, [&](Timeline *ptr) { new (ptr) Timeline(scope); },
            [&](const int64_t &k, Timeline &v) {
                v.insert(std::make_pair(timestamp, post_id), scope);
            },
            &scope);
    }

    // printf("%p insert mention hometimeline\n", fibre_self());
    for (auto id : follower_ids) {
        states_.userid_to_hometimeline_map.insert_or_update(
            id, [&](Timeline *ptr) { new (ptr) Timeline(scope); },
            [&](const int64_t &k, Timeline &v) {
                // printf("insert start\n");
                v.insert(std::make_pair(timestamp, post_id), scope);
                // printf("insert end\n");
            },
            &scope);
    }
    // printf("%p insert follower hometimeline\n", fibre_self());
}

// Follow: Allows a user to follow another user, updating the followee's and
// follower's lists accordingly.
void BackEndService::Follow(int64_t user_id, int64_t followee_id,
                            Beehive::DereferenceScope &scope, FollowReq *req) {
    Beehive::async::GenericStreamRunner<512> runner(scope);
    states_.userid_to_followees_map.insert_or_update(
        user_id,
        [&](FarSmallVector<int64_t> *ptr) {
            new (ptr) FarSmallVector<int64_t>(scope);
        },
        [&](const int64_t &k, FarSmallVector<int64_t> &v) {
            FollowCont cont(req);
            cont.runner = &runner;
            runner.async_run([&](void *ctx) {
                new (ctx)
                    FarSmallVector<int64_t>::InsertContext<true, FollowCont>(
                        &v, followee_id, std::move(cont));
            });
        },
        &scope);

    runner.await();
}

LocalVector<int64_t> BackEndService::GetFollowers(
    int64_t user_id, Beehive::DereferenceScope &scope) {
    Beehive::async::GenericStreamRunner<sizeof(
        OrderedVector<int64_t>::GetAllContext<false, GetFollowsCont>)>
        runner(scope);
    LocalVector<int64_t> ans;
    GetFollowsCont cont;
    cont.res = &ans;
    states_.userid_to_followers_map.get(
        user_id,
        [&](const int64_t &k, FarSmallVector<int64_t> &v) {
            runner.async_run([&](void *ctx) {
                new (ctx)
                    FarSmallVector<int64_t>::GetAllContext<false,
                                                           GetFollowsCont>(
                        &v, std::move(cont));
            });
        },
        &scope);
    runner.await();
    return ans;
}

LocalVector<int64_t> BackEndService::GetFollowees(
    int64_t user_id, Beehive::DereferenceScope &scope) {
    Beehive::async::GenericStreamRunner<sizeof(
        OrderedVector<int64_t>::GetAllContext<false, GetFollowsCont>)>
        runner(scope);
    LocalVector<int64_t> ans;
    GetFollowsCont cont;
    cont.res = &ans;
    states_.userid_to_followees_map.get(
        user_id,
        [&](const int64_t &k, FarSmallVector<int64_t> &v) {
            runner.async_run([&](void *ctx) {
                new (ctx)
                    FarSmallVector<int64_t>::GetAllContext<false,
                                                           GetFollowsCont>(
                        &v, std::move(cont));
            });
        },
        &scope);
    runner.await();
    return ans;
}

void BackEndService::RegisterUserWithId(std::string_view first_name,
                                        std::string_view last_name,
                                        std::string_view username,
                                        std::string_view password,
                                        int64_t user_id,
                                        Beehive::DereferenceScope &scope) {
    UserProfile user_profile;
    user_profile.first_name = first_name;
    user_profile.last_name = last_name;
    user_profile.user_id = user_id;
    auto salt = generate_random_string_util(SaltLen);
    user_profile.salt = salt;
    user_profile.password_hashed =
        picosha2::hash256_hex_string(std::string(password) + salt);
    Beehive::async::GenericStreamRunner<
        sizeof(ConcurrentHashMap<FixedSizeString<UserNameLen>,
                                 UserProfile>::PutContext<BoolCont>),
        true>
        runner(scope);
    bool res;
    runner.async_run([&](async::GenericOrderedContext *ctx) {
        BoolCont cont(&res);
        new (ctx) ConcurrentHashMap<FixedSizeString<UserNameLen>, UserProfile>::
            PutContext<BoolCont>(&states_.username_to_userprofile_map, username,
                                 user_profile, std::move(cont));
    });
    runner.await();
    if (!res) {
        ERROR("register fault");
    }
    // states_.username_to_userprofile_map.put(username, user_profile, scope);
}

// User Registration Method (if present, pseudocode)
// Registers a new user by adding their profile to a hash map.
// Hash map stores username to UserProfile mapping for quick lookup.
void BackEndService::RegisterUser(std::string_view first_name,
                                  std::string_view last_name,
                                  std::string_view username,
                                  std::string_view password,
                                  Beehive::DereferenceScope &scope) {
    auto user_id = (GenUniqueId() << 16);
    RegisterUserWithId(first_name, last_name, username, password, user_id,
                       scope);
}

// User Login Method (if present, pseudocode)
// Handles user login by checking provided credentials against stored profiles.
std::variant<LoginErrorCode, std::string> BackEndService::Login(
    std::string_view username, std::string_view password,
    Beehive::DereferenceScope &scope) {
    std::string signature;
    UserProfile userprofile;
    bool found;
    FixedSizeString<UserNameLen> username_temp = username;
    Beehive::async::GenericStreamRunner<sizeof(
        ConcurrentHashMap<FixedSizeString<UserNameLen>,
                          UserProfile>::GetContext<false, LoginCont<false>>)>
        runner(scope);
    runner.async_run([&](void *ctx) {
        LoginCont<false> cont(&userprofile, &found);
        new (ctx) ConcurrentHashMap<FixedSizeString<UserNameLen>, UserProfile>::
            GetContext<false, LoginCont<false>>(
                &states_.username_to_userprofile_map, &username_temp,
                std::move(cont));
    });
    runner.await();
    // auto found =
    //     states_.username_to_userprofile_map.get(username, &userprofile,
    //     scope);
    if (!found) {
        return NOT_REGISTERED;
    }

    if (VerifyLogin(signature, userprofile, username, password,
                    states_.secret)) {
        return signature;
    } else {
        return WRONG_PASSWORD;
    }
}

// never used
LocalVector<Post> BackEndService::ReadPosts(
    const LocalVector<int64_t> &post_ids, Beehive::DereferenceScope &scope) {
    LocalVector<Post> posts;
    for (auto post_id : post_ids) {
        Post post;
        auto found = states_.postid_to_post_map.get(post_id, &post, scope);
        if (found) {
            posts.emplace_back(std::move(post));
        }
    }
    return posts;
}

// never userd
void BackEndService::FollowWithUsername(std::string_view user_name,
                                        std::string_view followee_name,
                                        Beehive::DereferenceScope &scope) {
    std::cout << "never used" << std::endl;
    auto user_id = GetUserId(user_name, scope);
    auto followee_id = GetUserId(followee_name, scope);
    if (user_id && followee_id) {
        // Follow(user_id.value(), followee_id.value(), scope);
    }
}

// never userd
void BackEndService::UnfollowWithUsername(std::string_view user_name,
                                          std::string_view followee_name,
                                          Beehive::DereferenceScope &scope) {
    std::cout << "never used" << std::endl;
    auto user_id = GetUserId(user_name, scope);
    auto followee_id = GetUserId(followee_name, scope);
    if (user_id && followee_id) {
        Unfollow(user_id.value(), followee_id.value(), scope);
    }
}

// never userd
std::optional<int64_t> BackEndService::GetUserId(
    std::string_view username, Beehive::DereferenceScope &scope) {
    std::cout << "never used" << std::endl;
    UserProfile userprofile;
    states_.username_to_userprofile_map.get(username, &userprofile, scope);
    return userprofile.user_id;
}

// never userd
void BackEndService::UploadMedia(std::string_view filename,
                                 std::string_view data,
                                 Beehive::DereferenceScope &scope) {
    std::cout << "never used" << std::endl;
    states_.filename_to_data_map.put(filename, data, scope);
}

// never userd
FixedSizeString<DataLen> BackEndService::GetMedia(
    std::string_view filename, Beehive::DereferenceScope &scope) {
    std::cout << "never used" << std::endl;
    FixedSizeString<DataLen> filedata;
    bool found = states_.filename_to_data_map.get(filename, &filedata, scope);
    if (!found) {
        return std::string_view("");
    }
    return filedata;
}

// never userd
// Unfollow: Allows a user to unfollow another user, updating the lists
// accordingly.
void BackEndService::Unfollow(int64_t user_id, int64_t followee_id,
                              Beehive::DereferenceScope &scope) {
    std::cout << "never used" << std::endl;

    states_.userid_to_followees_map.insert_or_update(
        user_id,
        [&](FarSmallVector<int64_t> *ptr) {
            new (ptr) FarSmallVector<int64_t>(scope);
        },
        [&](const int64_t &k, FarSmallVector<int64_t> &v) {
            v.erase(followee_id, scope);
        },
        &scope);

    states_.userid_to_followers_map.insert_or_update(
        followee_id,
        [&](FarSmallVector<int64_t> *ptr) {
            new (ptr) FarSmallVector<int64_t>(scope);
        },
        [&](const int64_t &k, FarSmallVector<int64_t> &v) {
            v.erase(user_id, scope);
        },
        &scope);
}

// void BackEndService::computeHometimelineSize() {
//   size_t max_size = 0;
//   size_t total_size = 0;

//   states_.userid_to_hometimeline_map.forEach(
//       [&max_size, &total_size](const int64_t &key, Timeline &value) {
//         size_t timeline_size = value.size();
//         max_size = std::max(max_size, timeline_size);
//         total_size += timeline_size;
//       });

//   size_t total_entries = states_.userid_to_hometimeline_map.size();
//   double average_size =
//       total_entries > 0 ? static_cast<double>(total_size) / total_entries :
//       0.0;

//   std::cout << "Max Home Timeline size: " << max_size << std::endl;
//   std::cout << "Avg Home Timeline size: " << average_size << std::endl;
// }

// void BackEndService::computeUsertimelineSize() {
//   size_t max_size = 0;
//   size_t total_size = 0;

//   states_.userid_to_usertimeline_map.forEach(
//       [&max_size, &total_size](const int64_t &key, Timeline &value) {
//         size_t timeline_size = value.size();
//         max_size = std::max(max_size, timeline_size);
//         total_size += timeline_size;
//       });

//   size_t total_entries = states_.userid_to_usertimeline_map.size();
//   double average_size =
//       total_entries > 0 ? static_cast<double>(total_size) / total_entries :
//       0.0;

//   std::cout << "Max User Timeline size: " << max_size << std::endl;
//   std::cout << "Avg User Timeline size: " << average_size << std::endl;
// }

// void BackEndService::computeFollowerSize() {
//   size_t max_size = 0;
//   size_t total_size = 0;

//   states_.userid_to_followers_map.forEach(
//       [&max_size, &total_size](const int64_t &key,
//                                const OrderedVector<int64_t, compareInt64>
//                                &followers) {
//         size_t size = followers.size();
//         max_size = std::max(max_size, size);
//         total_size += size;
//       });

//   size_t total_entries = states_.userid_to_followers_map.size();
//   double average_size =
//       total_entries > 0 ? static_cast<double>(total_size) / total_entries :
//       0.0;
//   std::cout << "Max User Follower size: " << max_size << std::endl;
//   std::cout << "Avg User Follower size: " << average_size << std::endl;
// }

// void BackEndService::computeFolloweeSize() {
//   size_t max_size = 0;
//   size_t total_size = 0;

//   states_.userid_to_followees_map.forEach(
//       [&max_size, &total_size](const int64_t &key,
//                                const OrderedVector<int64_t, compareInt64>
//                                &followers) {
//         size_t size = followers.size();
//         max_size = std::max(max_size, size);
//         total_size += size;
//       });

//   size_t total_entries = states_.userid_to_followees_map.size();
//   double average_size =
//       total_entries > 0 ? static_cast<double>(total_size) / total_entries :
//       0.0;
//   std::cout << "Max User Followee size: " << max_size << std::endl;
//   std::cout << "Avg User Followee size: " << average_size << std::endl;

//   this->computeRatioSize();
//   this->computeUrlSize();
//   computeMentionSize();
// }

// void BackEndService::computeRatioSize() {
//   double max_ratio = 0;
//   double total_ratio = 0;

//   states_.userid_to_followers_map.forEach(
//       [&max_ratio, &total_ratio,
//        &userid_to_followees_map = states_.userid_to_followees_map](
//           const int64_t &key, const OrderedVector<int64_t, compareInt64>
//           &followers) {
//         size_t size_follower = followers.size();
//         auto followees_optional = userid_to_followees_map.getPoint(key);
//         if (followees_optional) {
//           auto followees = followees_optional.value();
//           size_t size_followee = followees->size();
//           double ratio = size_follower / size_followee;
//           max_ratio = std::max(max_ratio, ratio);
//           total_ratio += ratio;
//         }
//       });

//   double total_entries = states_.userid_to_followers_map.size();
//   double average_size = total_entries > 0
//                             ? static_cast<double>(total_ratio) /
//                             total_entries : 0.0;
//   std::cout << "Max User Follower Followee ratio: " << max_ratio <<
//   std::endl; std::cout << "Avg User Follower Followee ratio: " <<
//   average_size
//             << std::endl;
// }

// void BackEndService::computeUrlSize() {
//   size_t max_size = 0;
//   size_t total_size = 0;

//   states_.short_to_extended_map.forEach(
//       [&max_size, &total_size](std::string_view key,
//                                std::string_view value) {
//         size_t size = value.size();
//         max_size = std::max(max_size, size);
//         total_size += size;
//       });

//   size_t total_entries = states_.short_to_extended_map.size();
//   double average_size =
//       total_entries > 0 ? static_cast<double>(total_size) / total_entries :
//       0.0;
//   std::cout << "Max Url size: " << max_size << std::endl;
//   std::cout << "Avg Url size: " << average_size << std::endl;
// }

}  // namespace social_network
