#include "utils.hpp"

#include <boost/range/join.hpp>
#include <fstream>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <thread>

#include "defs.hpp"
#include "jwt/jwt.hpp"
#include "picosha2.h"

using namespace jwt::params;

namespace social_network {
double mention_size = 0;
double mention_count = 0;
size_t mention_max_size = 0;

bool VerifyLogin(std::string &signature, const UserProfile &user_profile,
                 std::string_view username, std::string_view password,
                 std::string_view sec) {
    auto salted = boost::join(password, user_profile.salt.view());
    if (picosha2::hash256_hex_string(salted) !=
        user_profile.password_hashed.view()) {
        return false;
    }
    auto user_id_str = std::to_string(user_profile.user_id);
    auto timestamp_str =
        std::to_string(duration_cast<std::chrono::seconds>(
                           system_clock::now().time_since_epoch())
                           .count());
    jwt::jwt_object obj{algorithm("HS256"), secret(sec),
                        payload({{"user_id", user_id_str},
                                 {"username", username},
                                 {"timestamp", timestamp_str},
                                 {"ttl", "3600"}})};
    signature = obj.signature();
    return true;
}

std::vector<std::string> MatchUrls(std::string_view text) {
    std::vector<std::string> urls;
    std::smatch m;
    std::regex e("(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)");
    std::string s(text);
    while (std::regex_search(s, m, e)) {
        auto url = m.str();
        if (url.length() >= UrlLen) [[unlikely]] {
            urls.emplace_back(url.substr(0, UrlLen - 1));
        } else {
            urls.emplace_back(url);
        }
        s = m.suffix().str();
    }
    return urls;
}

std::vector<std::string> MatchMentions(std::string_view text) {
    std::vector<std::string> mention_usernames;
    std::smatch m;
    std::regex e("@[a-zA-Z0-9-_]+");
    std::string s(text);
    while (std::regex_search(s, m, e)) {
        auto user_mention = m.str();
        user_mention = user_mention.substr(1, user_mention.length());
        mention_count += 1;
        mention_max_size = std::max(mention_max_size, user_mention.size());
        mention_size += user_mention.size();
        mention_usernames.emplace_back(user_mention);
        s = m.suffix().str();
    }
    return mention_usernames;
}

std::string ShortenUrlInText(std::string_view text,
                             std::vector<Url> target_urls) {
    if (target_urls.empty()) {
        return std::string(text);
    }

    std::ostringstream updated_text;
    std::string s(text);
    std::smatch m;
    std::regex e("(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)");
    int idx = 0;
    while (std::regex_search(s, m, e)) {
        updated_text << m.prefix().str()
                     << target_urls[idx].shortened_url.view();
        s = m.suffix().str();
        idx++;
    }
    updated_text << s;
    return updated_text.str();
}

int64_t GenUniqueId() {
    // auto now = std::chrono::high_resolution_clock::now().time_since_epoch();
    // auto nanos =
    //     std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();

    // auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());

    // int64_t uniqueId =
    //     (nanos << 20) ^ tid;  // 假设tid不会超过1M，进行调整以保持唯一性
    // return uniqueId;
    static std::atomic_int64_t unique_id(0);
    return unique_id.fetch_add(1);
}

void computeMentionSize() {
    std::cout << "Max Mention size: " << mention_max_size << std::endl;
    std::cout << "Avg Mention size: " << mention_size / mention_count
              << std::endl;
}
}  // namespace social_network
