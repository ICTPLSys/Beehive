#pragma once

#include <chrono>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "defs.hpp"

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

namespace social_network {

class RandomStringGenerator {
public:
    RandomStringGenerator()
        : rd(), gen(rd()), dis(97, 122) {}  // 构造函数初始化随机数生成器

    std::string Gen(uint32_t len) {
        std::cerr << "deprecated, dont use" << std::endl;
        exit(-1);
        std::string str;
        str.reserve(len);
        for (uint32_t i = 0; i < len; ++i) {
            str.push_back(
                static_cast<char>(dis(gen)));  // 生成随机字符并添加到字符串中
        }
        return str;
    }

private:
    std::random_device rd;  // 用于获得真随机数的设备
    std::mt19937 gen;       // 梅森旋转算法(Mersenne Twister)伪随机数生成器
    std::uniform_int_distribution<>
        dis;  // 均匀分布的整数生成器，范围是 ASCII 表中的小写字母
};

std::vector<std::string> MatchUrls(std::string_view text);
std::vector<std::string> MatchMentions(std::string_view text);
std::string ShortenUrlInText(std::string_view text,
                             std::vector<Url> target_urls);
bool VerifyLogin(std::string &signature, const UserProfile &user_profile,
                 std::string_view username, std::string_view password,
                 std::string_view secret);
int64_t GenUniqueId();

void computeMentionSize();

}  // namespace social_network
