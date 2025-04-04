#include <hdr/hdr_histogram.h>

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>

#include "SimpleBackEndServer.hpp"
#include "async/stream_runner.hpp"
#include "cache/cache.hpp"
#include "cache/region_remote_allocator.hpp"
#include "client.hpp"
#include "config.hpp"
#include "libfibre/fibre.h"
#include "request.hpp"
#include "test/fixed_size_string.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/parallel.hpp"
#include "utils/perf.hpp"
#include "utils/stats.hpp"
#include "utils/threads.hpp"
#include "utils/zipfian.hpp"

using namespace social_network;
using RequestQueue =
    boost::lockfree::queue<Request *, boost::lockfree::fixed_sized<false>>;
RequestQueue request_queue(128);
bool flag = false;

std::atomic<int> active_producers(kNumThreads);

// std::condition_variable queue_cond;
uthread::Condition request_cond;
uthread::Mutex request_cond_mutex;

Request *gen_random_req(SocialNetworkThreadState *state) {
    auto rand_int = (state->dist_1_100)(state->gen);
    if (rand_int <= kUserTimelinePercent) {
        auto *user_timeline_req = new ReadUserTimelineReq();
        user_timeline_req->user_id = (state->dist_1_numusers)(state->gen);
        user_timeline_req->start = (state->dist_1_100)(state->gen);
        user_timeline_req->stop = user_timeline_req->start + 1;
        return user_timeline_req;
    }
    rand_int -= kUserTimelinePercent;
    if (rand_int <= kHomeTimelinePercent) {
        auto *home_timeline_req = new ReadHomeTimelineReq();
        home_timeline_req->user_id = (state->dist_1_numusers)(state->gen);
        home_timeline_req->start = (state->dist_1_100)(state->gen);
        home_timeline_req->stop = home_timeline_req->start + 1;
        return home_timeline_req;
    }
    rand_int -= kHomeTimelinePercent;
    if (rand_int <= kComposePostPercent) {
        auto *compose_post_req = new ComposePostReq();
        compose_post_req->user_id = (state->dist_1_numusers)(state->gen);
        compose_post_req->username = std::string("username_") +
                                     std::to_string(compose_post_req->user_id);
        compose_post_req->text = random_string(PureTextLen, state);
        auto num_user_mentions = (state->dist_0_maxnummentions)(state->gen);
        for (uint32_t i = 0; i < num_user_mentions; i++) {
            auto mentioned_id = (state->dist_1_numusers)(state->gen);
            compose_post_req->text +=
                " @username_" + std::to_string(mentioned_id);
        }
        auto num_urls = (state->dist_0_maxnumurls)(state->gen);
        for (uint32_t i = 0; i < num_urls; i++) {
            compose_post_req->text +=
                " http://" + random_string(UrlLen - 10, state);
        }
        auto num_medias = (state->dist_0_maxnummedias)(state->gen);
        for (uint32_t i = 0; i < num_medias; i++) {
            compose_post_req->media_ids.emplace_back(
                (state->dist_0_maxint64)(state->gen));
            compose_post_req->media_types.push_back("png");
        }
        compose_post_req->post_type = social_network::PostType::POST;
        return compose_post_req;
    }
    rand_int -= kComposePostPercent;
    if (rand_int <= kRemovePostsPercent) {
        auto *follow_req = new FollowReq();
        follow_req->user_id = (state->dist_1_numusers)(state->gen);
        follow_req->followee_id = (state->dist_1_numusers)(state->gen);
        return follow_req;
    }

    auto *remove_posts_req = new RemovePostsReq();
    remove_posts_req->user_id = (state->dist_1_numusers)(state->gen);
    remove_posts_req->start = 0;
    remove_posts_req->stop = remove_posts_req->start + 1;
    return remove_posts_req;
}

ServiceResult service(Request *req, SimpleBackEndServer &backend,
                      FarLib::DereferenceScope &scope) {
    switch (req->get_type()) {
    case compose_post: {
        auto *cpr = dynamic_cast<ComposePostReq *>(req);
        backend.compose_post(cpr->username, cpr->user_id, cpr->text,
                             cpr->media_ids, cpr->media_types, cpr->post_type,
                             scope, cpr);
        break;
    }
    case read_user_timeline: {
        // read timeline, return posts in given time span
        // then query postid_to_post_map
        auto *rurt = dynamic_cast<ReadUserTimelineReq *>(req);
        auto postList = backend.read_user_timeline(rurt->user_id, rurt->start,
                                                   rurt->stop, scope, rurt);
        delete req;
        return postList;
    }
    case read_home_timeline: {
        // read userid_to_hometimeline_map in give span
        // then query postid_to_post_map
        auto *rht = dynamic_cast<ReadHomeTimelineReq *>(req);
        auto postList = backend.read_home_timeline(rht->user_id, rht->start,
                                                   rht->stop, scope, rht);
        delete req;
        return postList;
    }
    case login: {
        // read username_to_userprofile_map
        // compare password
        auto *lr = dynamic_cast<LoginReq *>(req);
        backend.login(lr->username, lr->password, scope);
        break;
    }
    case register_user: {
        // write username_to_userprofile_map
        auto *rur = dynamic_cast<RegisterUserReq *>(req);
        backend.register_user(rur->first_name, rur->last_name, rur->username,
                              rur->password, scope);
        break;
    }
    case register_user_with_id: {
        // write username_to_userprofile_map
        auto *rurwi = dynamic_cast<RegisterUserWithIdReq *>(req);
        backend.register_user_with_id(rurwi->first_name, rurwi->last_name,
                                      rurwi->username, rurwi->password,
                                      rurwi->user_id, scope);
        break;
    }
    case get_followers: {
        // read userid_to_followers_map
        auto *gfr = dynamic_cast<GetFollowersReq *>(req);
        auto followers = backend.get_followers(gfr->user_id, scope);
        delete req;
        return followers;
    }
    case get_followees: {
        // read userid_to_followees_map
        auto *gfer = dynamic_cast<GetFolloweesReq *>(req);
        auto followees = backend.get_followees(gfer->user_id, scope);
        delete req;
        return followees;
    }

    case follow: {
        // modify userid_to_followers_map & userid_to_followees_map
        auto *fr = dynamic_cast<FollowReq *>(req);
        backend.follow(fr->user_id, fr->followee_id, scope, fr);
        break;
    }

    case unfollow: {
        // never used
        // modify userid_to_followers_map & userid_to_followees_map
        auto *ur = dynamic_cast<UnfollowReq *>(req);
        backend.unfollow(ur->user_id, ur->followee_id, scope);
        break;
    }

    case follow_with_username: {
        // never used
        // read username_to_userprofile_map
        // then modify userid_to_followers_map & userid_to_followees_map
        auto *fwr = dynamic_cast<FollowWithUsernameReq *>(req);
        backend.follow_with_username(fwr->user_username, fwr->followee_username,
                                     scope);
        break;
    }

    case unfollow_with_username: {
        // never used
        // read username_to_userprofile_map
        // then modify userid_to_followers_map & userid_to_followees_map
        auto *uur = dynamic_cast<UnfollowWithUsernameReq *>(req);
        backend.unfollow_with_username(uur->user_username,
                                       uur->followee_username, scope);
        break;
    }

    case upload_media: {
        // never used
        auto *umr = dynamic_cast<UploadMediaReq *>(req);
        backend.upload_media(umr->filename, umr->data, scope);
        break;
    }
    case get_media: {
        // never used
        auto *gmr = dynamic_cast<GetMediaReq *>(req);
        auto media = backend.get_media(gmr->filename, scope);
        delete req;
        return std::string(media.view());
    }
    case remove_posts: {
        // FIXME: todo
        auto *rpr = dynamic_cast<RemovePostsReq *>(req);
        backend.remove_posts(rpr->user_id, rpr->start, rpr->stop, scope);
        break;
    }
    }
    delete req;
    return nullptr;
}

std::string generateRandomString(size_t length) {
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

std::string join(const std::vector<std::string> &vec,
                 const std::string &delim) {
    std::stringstream ss;
    for (size_t i = 0; i < vec.size(); ++i) {
        ss << vec[i];
        if (i != vec.size() - 1) {
            ss << delim;
        }
    }
    return ss.str();
}

// FIX COMPOSE POST
void updateComposePost(SimpleBackEndServer &backend, int user_id, int num_users,
                       FarLib::DereferenceScope &scope) {
    // Generate text
    std::string text = generateRandomString(256);

    // User mentions
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> users_dist(1, num_users);
    std::uniform_int_distribution<> mention_dist(0, kMaxNumMentionsPerText);
    std::uniform_int_distribution<> url_dist(0, kMaxNumUrlsPerText);
    auto mention_count = mention_dist(gen);
    auto url_count = url_dist(gen);
    for (int i = 0; i < mention_count; ++i) {
        text += " @username_" + std::to_string(users_dist(gen));
    }

    // URLs
    for (int i = 0; i < url_count; ++i) {
        text += " http://" + generateRandomString(64);
    }
    std::string_view text_view(text);

    // Media
    LocalVector<int64_t> media_ids;
    LocalVector<std::string> media_types;
    std::uniform_int_distribution<> media_dist(0, kMaxNumMediasPerText);
    std::uniform_int_distribution<int64_t> dis(1000000000, 9999999999);
    for (int i = 0; i < media_dist(gen); ++i) {
        int64_t media_id = dis(gen);
        media_ids.push_back(media_id);
        media_types.push_back("png");
    }

    auto *compose_post_req = new ComposePostReq();

    backend.compose_post(
        "username_" + std::to_string(user_id), user_id, text_view, media_ids,
        media_types, social_network::PostType::TEXT, scope, compose_post_req);
}

void producer_thread_function(std::pair<int, std::chrono::seconds> *args) {
    auto start_time = std::chrono::steady_clock::now();
    struct SocialNetworkThreadState state;
    int64_t request_count = 100000;
    while (request_count--) {
        // Generate a random request
        auto *random_req = gen_random_req(&state);
        if (random_req) {
            // record statt ns
            random_req->request_start_ns = get_time_ns();
            // FIXME: use N queues
            request_queue.push(random_req);
        }
    }

    active_producers--;
    uthread::notify(&request_cond, &request_cond_mutex);
}

void producer_thread_function_new(int producer_id, struct config config) {
    // FIXME: this is another version for producer_thread_function
    uint64_t op_duration_ns = config.op_duration_ns;
    uint64_t op_speed_reset_ns = config.op_speed_reset_ns;
    uint64_t max_runtime_ns = config.max_runtime_ns;
    std::default_random_engine random_engine(std::random_device{}());
    std::exponential_distribution req_interval_dist(1.0 / op_duration_ns);
    uint64_t period_start_time = get_time_ns();
    uint64_t final_deadline = period_start_time + max_runtime_ns;
    uint64_t period_deadline = period_start_time + op_speed_reset_ns;
    uint64_t next_op_start_time = period_start_time;
    uint64_t op_count = 0;
    struct SocialNetworkThreadState state;
    while (op_count < config.max_request_count) {
        auto *random_req = gen_random_req(&state);
        if (random_req) {
            // record statt ns
            random_req->request_start_ns = get_time_ns();
            request_queue.push(random_req);
        }
        op_count++;
        next_op_start_time += req_interval_dist(random_engine);
        // wait until time to send next request
        uint64_t current_time;
        while ((current_time = get_time_ns()) < next_op_start_time) {
        }
        // break if timeout
        if (current_time >= final_deadline) break;
        // reset deadlines if last period finished
        if (current_time >= period_deadline) [[unlikely]] {
            period_start_time = current_time;
            period_deadline = current_time + op_speed_reset_ns;
            next_op_start_time = period_start_time;
        }
    }

    active_producers--;
    uthread::notify(&request_cond, &request_cond_mutex);
}

void consumer_uthread_function(SimpleBackEndServer *backend) {
    Request *request;
    int num = 0;
    int64_t start, stop;
    start = get_time_ns();
    hdr_histogram *hist_in_queue = nullptr;
    hdr_histogram *hist_serve = nullptr;
    // FIXME: use global hdr records
    hdr_init(1, 100'000'000, 3, &hist_in_queue);
    hdr_init(1, 100'000'000, 3, &hist_serve);
    FarLib::RootDereferenceScope scope;
    while (true) {
        if (request_queue.empty() && active_producers.load() == 0) {
            break;
        }
        // FIXME: async
        while (request_queue.pop(request)) {
            hdr_record_value(hist_in_queue,
                             get_time_ns() - request->request_start_ns);
            service(request, *backend, scope);
            num++;
            hdr_record_value(hist_serve,
                             get_time_ns() - request->request_start_ns);
        }
    }

    stop = get_time_ns();

    std::cout << "queue latency mean " << hdr_mean(hist_in_queue) << "ns"
              << std::endl;
    std::cout << "queue latency p50 "
              << hdr_value_at_percentile(hist_in_queue, 50) << "ns"
              << std::endl;
    std::cout << "queue latency p90 "
              << hdr_value_at_percentile(hist_in_queue, 90) << "ns"
              << std::endl;
    std::cout << "queue latency p95 "
              << hdr_value_at_percentile(hist_in_queue, 95) << "ns"
              << std::endl;
    std::cout << "queue latency p99 "
              << hdr_value_at_percentile(hist_in_queue, 99) << "ns"
              << std::endl;
    double interval = (stop - start) / 1000000000;
    std::cout << "serve throughout " << hist_in_queue->total_count / interval
              << std::endl;
    std::cout << "serve latency mean " << hdr_mean(hist_serve) << "ns"
              << std::endl;
    std::cout << "serve latency p50 " << hdr_value_at_percentile(hist_serve, 50)
              << "ns" << std::endl;
    std::cout << "serve latency p90 " << hdr_value_at_percentile(hist_serve, 90)
              << "ns" << std::endl;
    std::cout << "serve latency p95 " << hdr_value_at_percentile(hist_serve, 95)
              << "ns" << std::endl;
    std::cout << "serve latency p99 " << hdr_value_at_percentile(hist_serve, 99)
              << "ns" << std::endl;
    // std::cout << "Consumer is stopping consuming." << std::endl;

    hdr_close(hist_in_queue);
    hdr_close(hist_serve);
}

int run(const char *graph_file) {
    std::ifstream graphFile(graph_file);
    if (!graphFile.is_open()) {
        std::cerr << "Error opening file" << std::endl;
        return 1;
    }

    SimpleBackEndServer backend;
    std::string line;
    std::getline(graphFile, line);
    int num_users = std::stoi(line);
    // std::cout << num_users << std::endl;

    {
        FarLib::RootDereferenceScope scope;
        /* prepare data */
        std::cout << "register user" << std::endl;
        // Register users
        for (int i = 1; i <= num_users; ++i) {
            std::string user_id = std::to_string(i);
            RegisterUserWithIdReq *req =
                new RegisterUserWithIdReq("First" + user_id, "Last" + user_id,
                                          "username_" + user_id, "password", i);
            service(req, backend, scope);
        }

        std::cout << "follow" << std::endl;
        // followers
        while (std::getline(graphFile, line)) {
            std::istringstream iss(line);
            int64_t user_0, user_1;
            if (!(iss >> user_0 >> user_1)) {
                break;
            }
            FollowReq *req = new FollowReq(user_0, user_1);
            service(req, backend, scope);
        }

        std::cout << "post initial" << std::endl;
        // posts
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> post_dist(0, 10);
        size_t count = 0;
        for (int user_id = 1; user_id <= num_users; ++user_id) {
            int num_posts = post_dist(gen);

            for (int post = 0; post < num_posts; ++post, ++count) {
                updateComposePost(backend, user_id, num_users, scope);
                if (count % 10000 == 0) {
                    std::cout << "compose post " << count << std::endl;
                }
            }
        }
    }
    // init done

    // Create multiple producer threads
    const int num_producers = kNumThreads;  // For example, 3 producers
    std::vector<std::unique_ptr<FarLib::uthread::UThread>> producer_threads(
        num_producers);
    for (int i = 0; i < num_producers; ++i) {
        std::pair<int, std::chrono::seconds> args = {i + 1,
                                                     std::chrono::seconds(1)};
        producer_threads[i] =
            FarLib::uthread::create(producer_thread_function, &args);
    }

    std::unique_ptr<FarLib::uthread::UThread> uthread_consumer;
    // FIXME: multi consumers
    uthread_consumer =
        FarLib::uthread::create(consumer_uthread_function, &backend);

    // Join producer threads (after they finish producing)
    for (auto &thread : producer_threads) {
        std::cout << "join producer thread" << std::endl;
        FarLib::uthread::join(std::move(thread));
        std::cout << "join producer thread success" << std::endl;
    }

    // Join the single consumer thread
    std::cout << "join consumer thread" << std::endl;
    FarLib::uthread::join(std::move(uthread_consumer));
    std::cout << "join consumer thread success" << std::endl;

    // backend.computeHometimelineSize();
    // backend.computeUsertimelineSize();
    // backend.computeFollowerSize();
    // backend.computeFolloweeSize();
    States::destroyInstance();
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        std::cout << "usage: " << argv[0] << " <configure file> <graph file>"
                  << std::endl;
        return -1;
    }

    FarLib::perf_init();
    FarLib::rdma::Configure config;
    config.from_file(argv[1]);
    ASSERT(config.max_thread_cnt >= 2);
    ASSERT(config.max_thread_cnt % 2 == 0);
    std::cout << "config: client buffer size = " << config.client_buffer_size
              << std::endl;
    FarLib::runtime_init(config);
    std::cout << "running server on " << config.max_thread_cnt / 2 << " cores"
              << std::endl;
    int res = run(argv[2]);
    FarLib::runtime_destroy();
    return res;
}
