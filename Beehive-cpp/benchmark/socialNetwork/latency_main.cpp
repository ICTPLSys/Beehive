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
#include <string>
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
#include "utils/cpu_cycles.hpp"
#include "utils/debug.hpp"
#include "utils/parallel.hpp"
#include "utils/perf.hpp"
#include "utils/profile.hpp"
#include "utils/stats.hpp"
#include "utils/uthreads.hpp"
#include "utils/zipfian.hpp"

using namespace social_network;
using namespace FarLib;

// std::condition_variable queue_cond;

static uint64_t MaxQueueTime = 1024 * 1024 * 1024;
static std::string csv_postfix;
struct Response {
    uint64_t in_queue_lat_ns;
    uint64_t service_latency_ns;
};

struct Config {
    double zipfian_constant;
    size_t max_request_count;
    std::chrono::nanoseconds max_runtime;
    std::chrono::nanoseconds op_duration;
    std::chrono::nanoseconds op_speed_reset;
};

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
        auto *remove_posts_req = new RemovePostsReq();
        remove_posts_req->user_id = (state->dist_1_numusers)(state->gen);
        remove_posts_req->start = 0;
        remove_posts_req->stop = remove_posts_req->start + 1;
        return remove_posts_req;
    }
    auto *follow_req = new FollowReq();
    follow_req->user_id = (state->dist_1_numusers)(state->gen);
    follow_req->followee_id = (state->dist_1_numusers)(state->gen);
    return follow_req;
}

class SocialNetworkServer {
public:
    using RequestQueue =
        boost::lockfree::queue<Request *, boost::lockfree::capacity<64>>;

protected:
    std::atomic_bool serving;
    RequestQueue request_queue[NQueue];
    hdr_histogram *hist_in_queue = nullptr;
    hdr_histogram *hist_serve = nullptr;

public:
    struct Stats {
        int64_t served_count;
        double q_mean;
        int64_t q_p50;
        int64_t q_p90;
        int64_t q_p95;
        int64_t q_p99;
        double s_mean;
        int64_t s_p50;
        int64_t s_p90;
        int64_t s_p95;
        int64_t s_p99;
    };

    Stats get_stats() {
        return Stats{
            .served_count = hist_in_queue->total_count,
            .q_mean = hdr_mean(hist_in_queue),
            .q_p50 = hdr_value_at_percentile(hist_in_queue, 50),
            .q_p90 = hdr_value_at_percentile(hist_in_queue, 90),
            .q_p95 = hdr_value_at_percentile(hist_in_queue, 95),
            .q_p99 = hdr_value_at_percentile(hist_in_queue, 99),
            .s_mean = hdr_mean(hist_serve),
            .s_p50 = hdr_value_at_percentile(hist_serve, 50),
            .s_p90 = hdr_value_at_percentile(hist_serve, 90),
            .s_p95 = hdr_value_at_percentile(hist_serve, 95),
            .s_p99 = hdr_value_at_percentile(hist_serve, 99),
        };
    }

public:
    SocialNetworkServer() : serving(true) {
        int err;
        err = hdr_init(1, 10'000'000, 3, &hist_in_queue);
        ASSERT(err == 0);
        err = hdr_init(1, 10'000'000, 3, &hist_serve);
        ASSERT(err == 0);
    }

    ~SocialNetworkServer() {
        hdr_close(hist_in_queue);
        hdr_close(hist_serve);
    }

    virtual void serve(size_t qi, size_t id) = 0;

    bool send_request(size_t qi, Request *req) {
        req->request_start_ns = get_time_ns();

        bool sent = request_queue[qi].push(req);

        door_bell(qi);
        return sent;
    }

    virtual void door_bell(size_t qi) {}

    virtual void finish_serving() { serving.store(false); }

    void print_cdf(size_t i) const {
        std::string hq_name =
            "hq" + std::to_string(i) + "-" + csv_postfix + ".classic";
        std::string hs_name =
            "hs" + std::to_string(i) + "-" + csv_postfix + ".classic";
        FILE *fq = fopen(hq_name.c_str(), "w");
        hdr_percentiles_print(hist_in_queue, fq, 100, 1, CLASSIC);
        FILE *fs = fopen(hs_name.c_str(), "w");
        hdr_percentiles_print(hist_serve, fs, 100, 1, CLASSIC);
        fclose(fq);
        fclose(fs);
    }

protected:
    bool get_request(size_t qi, Request *&request, Response &response) {
        while (request_queue[qi].pop(request)) {
            if (request->request_start_ns + MaxQueueTime < get_time_ns())
                continue;
            response.in_queue_lat_ns =
                get_time_ns() - request->request_start_ns;
            return true;
        }
        return false;
    }

    void send_response(Request *request, Response &response) {
        response.service_latency_ns = get_time_ns() - request->request_start_ns;
        hdr_record_value_atomic(hist_in_queue, response.in_queue_lat_ns);
        hdr_record_value_atomic(hist_serve, response.service_latency_ns);
        delete request;
    }

    bool is_serving() { return serving.load(std::memory_order::relaxed); }
};

class UThreadServer : public SocialNetworkServer {
private:
    uthread::Condition request_cond[NQueue];
    uthread::Mutex request_mutex[NQueue];
    SimpleBackEndServer *backend;

public:
    UThreadServer(SimpleBackEndServer &backend) : backend(&backend) {}
    void door_bell(size_t qi) override {
        uthread::notify_all(&request_cond[qi], &request_mutex[qi]);
    }

    void finish_serving() override {
        serving.store(false);
        for (int64_t i = 0; i < NQueue; i++) {
            uthread::notify_all(&request_cond[i], &request_mutex[i]);
        }
    }

    void serve(size_t qi, size_t id) {
        if (serving.load()) {
            uthread::wait(&request_cond[qi], &request_mutex[qi]);
        }
        while (is_serving()) {
            Request *request;
            Response response;
            while (get_request(qi, request, response)) {
                backend->serve_request(request);
                send_response(request, response);
            }
            if (serving.load()) {
                // printf("queue empty, server %zu wait\n", id);
                uthread::wait(&request_cond[qi], &request_mutex[qi]);
                // printf("server %zu wakeup, continue\n", id);
            }
        }
        // printf("serve %zu end\n", id);
    }
};

uint64_t gen_requests(UThreadServer &server, SocialNetworkThreadState &state,
                      const Config &config, size_t qi,
                      size_t max_request_count) {
    uint64_t op_duration_ns = config.op_duration.count();
    uint64_t op_speed_reset_ns = config.op_speed_reset.count();
    uint64_t max_runtime_ns = config.max_runtime.count();
    assert(op_duration_ns < op_speed_reset_ns);
    assert(op_duration_ns < max_runtime_ns);
    std::default_random_engine random_engine(std::random_device{}());
    std::exponential_distribution req_interval_dist(1.0 / op_duration_ns);
    std::uniform_real_distribution<float> op_type_dist(0.0, 1.0);
    uint64_t period_start_time = get_time_ns();
    uint64_t final_deadline = period_start_time + max_runtime_ns;
    uint64_t period_deadline = period_start_time + op_speed_reset_ns;
    uint64_t next_op_start_time = period_start_time;
    uint64_t op_count = 0;
    while (op_count < max_request_count) {
        Request *request = gen_random_req(&state);
        // send request until success
        while (true) {
            bool sent = server.send_request(qi, request);
            if (sent) {
                break;
            }
        }
        op_count++;
        next_op_start_time += req_interval_dist(random_engine);
        // wait until time to send next request
        uint64_t current_time;
        while ((current_time = get_time_ns()) < next_op_start_time);
        // do not break if time out
        // we should process all requests
        // if (current_time >= final_deadline) break;
        // reset deadlines if last period finished
        if (current_time >= period_deadline) {
            period_start_time = current_time;
            period_deadline = current_time + op_speed_reset_ns;
            next_op_start_time = period_start_time;
        }
    }
    return op_count;
}

const static char *const ColumnNames[] = {
    "goal-lat", "op-duration", "run-time", "req-cnt", "serv-cnt",
    "q-mean",   "q-p50",       "q-p90",    "q-p95",   "q-p99",
    "s-mean",   "s-p50",       "s-p90",    "s-p95",   "s-p99"};
constexpr static size_t ColumnWidth = 12;
constexpr static size_t ColumnCount =
    sizeof(ColumnNames) / sizeof(*ColumnNames);

ServiceResult service(Request *req, SimpleBackEndServer &backend,
                      FarLib::DereferenceScope &scope) {
    switch (req->get_type()) {
    case COMPOSE_POST: {
        auto *cpr = reinterpret_cast<ComposePostReq *>(req);
        backend.compose_post(cpr->username, cpr->user_id, cpr->text,
                             cpr->media_ids, cpr->media_types, cpr->post_type,
                             scope, cpr);
        break;
    }
    case READ_USERTIMELINE: {
        // read timeline, return posts in given time span
        // then query postid_to_post_map
        auto *rurt = reinterpret_cast<ReadUserTimelineReq *>(req);
        auto postList = backend.read_user_timeline(rurt->user_id, rurt->start,
                                                   rurt->stop, scope, rurt);
        return postList;
    }
    case READ_HOMETIMELINE: {
        // read userid_to_hometimeline_map in give span
        // then query postid_to_post_map
        auto *rht = reinterpret_cast<ReadHomeTimelineReq *>(req);
        auto postList = backend.read_home_timeline(rht->user_id, rht->start,
                                                   rht->stop, scope, rht);
        return postList;
    }
    case LOGIN: {
        // read username_to_userprofile_map
        // compare password
        auto *lr = reinterpret_cast<LoginReq *>(req);
        backend.login(lr->username, lr->password, scope);
        break;
    }
    case REGISTER_USER: {
        // write username_to_userprofile_map
        auto *rur = reinterpret_cast<RegisterUserReq *>(req);
        backend.register_user(rur->first_name, rur->last_name, rur->username,
                              rur->password, scope);
        break;
    }
    case REGISTER_USER_WITH_ID: {
        // write username_to_userprofile_map
        auto *rurwi = reinterpret_cast<RegisterUserWithIdReq *>(req);
        backend.register_user_with_id(rurwi->first_name, rurwi->last_name,
                                      rurwi->username, rurwi->password,
                                      rurwi->user_id, scope);
        break;
    }
    case GET_FOLLOWERS: {
        // read userid_to_followers_map
        auto *gfr = reinterpret_cast<GetFollowersReq *>(req);
        auto followers = backend.get_followers(gfr->user_id, scope);
        return followers;
    }
    case GET_FOLLOWEES: {
        // read userid_to_followees_map
        auto *gfer = reinterpret_cast<GetFolloweesReq *>(req);
        auto followees = backend.get_followees(gfer->user_id, scope);
        return followees;
    }

    case FOLLOW: {
        // modify userid_to_followers_map & userid_to_followees_map
        auto *fr = reinterpret_cast<FollowReq *>(req);
        backend.follow(fr->user_id, fr->followee_id, scope, fr);
        break;
    }

    case UNFOLLOW: {
        // never used
        // modify userid_to_followers_map & userid_to_followees_map
        auto *ur = reinterpret_cast<UnfollowReq *>(req);
        backend.unfollow(ur->user_id, ur->followee_id, scope);
        break;
    }

    case FOLLOW_WITH_USERNAME: {
        // never used
        // read username_to_userprofile_map
        // then modify userid_to_followers_map & userid_to_followees_map
        auto *fwr = reinterpret_cast<FollowWithUsernameReq *>(req);
        backend.follow_with_username(fwr->user_username, fwr->followee_username,
                                     scope);
        break;
    }

    case UNFOLLOW_WITH_USERNAME: {
        // never used
        // read username_to_userprofile_map
        // then modify userid_to_followers_map & userid_to_followees_map
        auto *uur = reinterpret_cast<UnfollowWithUsernameReq *>(req);
        backend.unfollow_with_username(uur->user_username,
                                       uur->followee_username, scope);
        break;
    }

    case UPLOAD_MEDIA: {
        // never used
        auto *umr = reinterpret_cast<UploadMediaReq *>(req);
        backend.upload_media(umr->filename, umr->data, scope);
        break;
    }
    case GET_MEDIA: {
        // never used
        auto *gmr = reinterpret_cast<GetMediaReq *>(req);
        auto media = backend.get_media(gmr->filename, scope);
        return std::string(media.view());
    }
    case REMOVE_POSTS: {
        // FIXME: todo
        auto *rpr = reinterpret_cast<RemovePostsReq *>(req);
        backend.remove_posts(rpr->user_id, rpr->start, rpr->stop, scope);
        break;
    }
    }
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
    std::mt19937 gen(user_id);
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

struct ServerArgs {
    UThreadServer *server;
    size_t i;

    ServerArgs(UThreadServer *server, size_t i) : server(server), i(i) {}
};

void server_function(ServerArgs *server_arg) {
    server_arg->server->serve(server_arg->i % NQueue, server_arg->i);
}

struct ClientArgs {
    SimpleBackEndServer *backend;
    UThreadServer *server;
    const Config *config;
    std::atomic_uint64_t *req_count;
    size_t i;
    size_t num_users;
    size_t target_req_count;

    ClientArgs(SimpleBackEndServer *backend, UThreadServer *server,
               const Config *config, std::atomic_uint64_t *req_count, size_t i,
               size_t num_users, size_t target_req_count)
        : backend(backend),
          server(server),
          config(config),
          req_count(req_count),
          i(i),
          num_users(num_users),
          target_req_count(target_req_count) {}
};

void client_function(ClientArgs *client_arg) {
    SocialNetworkThreadState state(client_arg->num_users, client_arg->i);
    client_arg->req_count->fetch_add(
        gen_requests(*(client_arg->server), state, *(client_arg->config),
                     client_arg->i, client_arg->target_req_count));
}

bool evaluate(size_t test_count, SimpleBackEndServer &backend,
              const Config &config, size_t num_users, bool verbose = true,
              size_t num_server_threads = kNumServerThreads) {
    UThreadServer server(backend);
    size_t num_client_threads = std::min(NQueue, num_server_threads);
    std::vector<std::unique_ptr<uthread::UThread>> server_threads(
        num_server_threads);
    std::vector<std::unique_ptr<ServerArgs>> server_args(num_server_threads);
    std::vector<std::unique_ptr<uthread::UThread>> client_threads(
        num_client_threads);
    std::vector<std::unique_ptr<ClientArgs>> client_args(num_client_threads);
    std::atomic_uint64_t req_count(0);
    auto start_ns = get_time_ns();
    FarLib::perf_profile([&] {
        FarLib::profile::beehive_profile([&] {
            for (size_t i = 0; i < num_server_threads; i++) {
                server_args[i].reset(new ServerArgs(&server, i));
                server_threads[i] = std::move(
                    uthread::create(server_function, server_args[i].get()));
            }
            for (size_t i = 0; i < num_client_threads; i++) {
                client_args[i].reset(new ClientArgs(&backend, &server, &config,
                                                    &req_count, i, num_users,
                                                    RequestPerClient));
                client_threads[i] = std::move(
                    uthread::create(client_function, client_args[i].get()));
            }
            for (auto &client_thread : client_threads) {
                uthread::join(std::move(client_thread));
            }

            server.finish_serving();
            for (auto &server_thread : server_threads) {
                uthread::join(std::move(server_thread));
            }
        });
    }).print();

    auto end_ns = get_time_ns();
    auto stats = server.get_stats();
    if (verbose) {
        double run_time = (double)(end_ns - start_ns) / 1e9;
        std::cout << std::setw(ColumnWidth) << MaxQueueTime;
        std::cout << std::setw(ColumnWidth) << config.op_duration.count();
        std::cout << std::setw(ColumnWidth) << run_time;
        std::cout << std::setw(ColumnWidth) << req_count;
        std::cout << std::setw(ColumnWidth) << stats.served_count;
        std::cout << std::setw(ColumnWidth) << stats.q_mean;
        std::cout << std::setw(ColumnWidth) << stats.q_p50;
        std::cout << std::setw(ColumnWidth) << stats.q_p90;
        std::cout << std::setw(ColumnWidth) << stats.q_p95;
        std::cout << std::setw(ColumnWidth) << stats.q_p99;
        std::cout << std::setw(ColumnWidth) << stats.s_mean;
        std::cout << std::setw(ColumnWidth) << stats.s_p50;
        std::cout << std::setw(ColumnWidth) << stats.s_p90;
        std::cout << std::setw(ColumnWidth) << stats.s_p95;
        std::cout << std::setw(ColumnWidth) << stats.s_p99;
        std::cout << std::endl;
        // server.print_cdf(test_count);
    }
    // backend.compute_hometimeline_size();
    // backend.compute_usertimeline_size();
    // backend.compute_follower_size();
    // backend.compute_followee_size();
    return stats.s_p99 <= MaxQueueTime;
}

int run(const char *graph_file) {
    std::cout << "size of post: " << sizeof(Post) << std::endl;
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
        /* prepare data */
        std::cout << "register user" << std::endl;
        // Register users
        static size_t user_count = 0;
        uthread::parallel_for_with_scope<1>(
            kNumPreProcessors, num_users,
            [&](size_t i, DereferenceScope &scope) {
                std::string user_id = std::to_string(i + 1);
                RegisterUserWithIdReq *req = new RegisterUserWithIdReq(
                    "First" + user_id, "Last" + user_id, "username_" + user_id,
                    "password", i);
                service(req, backend, scope);
                user_count++;
                if (user_count % 10000 == 0) {
                    printf("login user %lu\n", user_count);
                }
            });

        std::cout << "follow" << std::endl;
        // followers

        auto my_getline = [&](std::ifstream &graph, std::string &line) {
            static uthread::Mutex mtx;
            uthread::lock(&mtx);
            bool is_eof = std::getline(graph, line).eof();
            uthread::unlock(&mtx);
            return !is_eof;
        };
        uthread::parallel_for_with_scope<1>(
            kNumPreProcessors, kNumPreProcessors,
            [&](size_t i, DereferenceScope &scope) {
                while (my_getline(graphFile, line)) {
                    std::istringstream iss(line);
                    int64_t user_0, user_1;
                    if (!(iss >> user_0 >> user_1)) {
                        break;
                    }
                    FollowReq *req = new FollowReq(user_0, user_1);
                    service(req, backend, scope);
                }
            });

        std::cout << "post initial" << std::endl;
        // posts
        static size_t count = 0;
        uthread::parallel_for_with_scope<1>(
            kNumPreProcessors, num_users,
            [&](size_t i, DereferenceScope &scope) {
                int user_id = i + 1;
                std::mt19937 gen(user_id);
                std::uniform_int_distribution<> post_dist(0, 8);
                int num_posts = post_dist(gen);
                for (int post = 0; post < num_posts; ++post, ++count) {
                    updateComposePost(backend, user_id, num_users, scope);
                    if (count % 100000 == 0) {
                        printf("compose post %zu\n", count);
                    }
                }
            });
    }
    using namespace std::chrono_literals;
    Config config{
        .zipfian_constant = 0.5,
        .max_request_count = MaxRequestCount,
        .max_runtime = 5s,
        .op_duration = 2us,
        .op_speed_reset = 100ms,
    };
    csv_postfix = std::to_string(config.op_duration.count()) + "us";
    // constexpr std::chrono::nanoseconds GoalLatency[] = {
    //     400us, 350us, 300us, 250us, 200us, 150us, 100us, 90us, 80us,
    //     70us,  60us,  50us,  45us,  40us,  35us,  30us,  25us, 20us,
    //     15us,  10us,  9us,   8us,   7us,   6us,   5us,   4us};

    constexpr std::chrono::nanoseconds GoalLatency[] = {400s};
    std::cout << "Warm up..." << std::endl;
    sleep(1);
    evaluate(0, backend, config, num_users, false);
    std::cout << "Bench..." << std::endl;
    for (auto name : ColumnNames) {
        std::cout << std::setw(ColumnWidth) << name;
    }
    std::cout << std::endl;
    FarLib::allocator::remote::remote_global_heap.info();
    for (auto g : GoalLatency) {
        MaxQueueTime = g.count();
        size_t loop_count = 15;
        for (size_t i = 0; i < loop_count; i++) {
            bool goal_achieved = evaluate(i, backend, config, num_users);
            // if (goal_achieved) break;
            // config.op_duration =
            //     int64_t(config.op_duration.count() * 1.1) * 1ns;
        }
    }
    States::destroyInstance();
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 3 && argc != 4) {
        std::cout << "usage: " << argv[0]
                  << " <configure file> <graph file> <local mem size>"
                  << std::endl;
        return -1;
    }

    FarLib::perf_init();
    FarLib::rdma::Configure config;
    config.from_file(argv[1]);
    if (argc >= 4) {
        config.client_buffer_size = std::stoul(argv[3]);
    }
    // ASSERT(config.max_thread_cnt >= 2);
    // ASSERT(config.max_thread_cnt % 2 == 0);
    std::cout << "config: client buffer size = " << config.client_buffer_size
              << std::endl;
    FarLib::runtime_init(config);
    std::cout << "running server on " << config.max_thread_cnt / 2 << " cores"
              << std::endl;
    int res = run(argv[2]);
    FarLib::runtime_destroy();
    return res;
}
