#pragma once
#include <cstdint>
constexpr static uint64_t kNumThreads = 4;
constexpr static double kTargetMops = 0.1;
constexpr static double kTotalMops = 1;
constexpr static uint64_t kNumEntries = 1;
constexpr static uint64_t kEntryPort = 9091;
constexpr static uint64_t kUserTimelinePercent = 60;
constexpr static uint64_t kHomeTimelinePercent = 30;
constexpr static uint64_t kComposePostPercent = 5;
constexpr static uint64_t kRemovePostsPercent = 5;
constexpr static uint64_t kFollowPercent =
    100 - kUserTimelinePercent - kHomeTimelinePercent - kComposePostPercent -
    kRemovePostsPercent;
constexpr static uint64_t kNumPreProcessors = 16;
constexpr static uint64_t NQueue = 4;
constexpr static uint64_t kNumServerThreads = 12;
static_assert(kNumServerThreads % NQueue == 0);
constexpr static char kCharSet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";
constexpr static uint64_t kMaxNumMentionsPerText = 2;
constexpr static uint64_t kMaxNumUrlsPerText = 2;
constexpr static uint64_t kMaxNumMediasPerText = 2;
constexpr static uint64_t kTimeSeriesIntervalUs = 10 * 1000;
constexpr size_t MaxRequestCount = 1024 * 1024 * 1024;
constexpr uint32_t NumEntriesShift = 25;
constexpr size_t RequestPerClient = 2'000'000;
