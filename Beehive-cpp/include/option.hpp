#pragma once
#include <cstddef>
#include <unordered_map>
enum Algorithm { DEFAULT, UTHREAD, PARAROUTINE, PREFETCH };
static constexpr double UTH_FACTOR = 4;
static constexpr Algorithm DEFAULT_ALG = PARAROUTINE;
static constexpr bool DEFAULT_TRIVIAL_OPT = true;

static std::unordered_map<Algorithm, const char *> alg2str = {
    {DEFAULT, "default"},
    {UTHREAD, "uthread"},
    {PARAROUTINE, "pararoutine"},
    {PREFETCH, "prefetch"}};
inline std::string get_alg_str(Algorithm alg = DEFAULT_ALG) {
    return alg2str[alg];
}