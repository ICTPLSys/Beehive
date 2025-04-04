#ifndef FIXED_SIZE_STRING
#define FIXED_SIZE_STRING

#include <cstdlib>
#include <cstring>
#include <functional>
#include <string_view>

namespace FarLib {

template <size_t Size>
struct FixedSizeString {
    std::array<char, Size + 1> str;

    FixedSizeString() = default;
    FixedSizeString(std::string_view s) {
        assert(s.size() <= Size);
        std::memcpy(str.data(), s.data(), std::min(s.length(), str.size()));
        str[s.size() + 1] = '\0';
    }
    FixedSizeString& operator=(std::string_view s) {
        assert(s.size() <= Size);
        std::memcpy(str.data(), s.data(), std::min(s.length(), str.size()));
        str[s.size() + 1] = '\0';
        return *this;
    }

    static FixedSizeString<Size> random() {
        FixedSizeString rand_str;
        for (size_t i = 0; i < Size; i++) {
            rand_str.str[i] = rand() % 0xFF;
        }
        return rand_str;
    }

    char& operator[](size_t i) { return str[i]; }

    bool operator==(const FixedSizeString<Size>& other) const {
        return std::strcmp(str.data(), other.str.data()) == 0;
    }

    std::string_view view() const { return {str.data(), str.size()}; }
};

}  // namespace FarLib

template <size_t Size>
struct std::hash<FarLib::FixedSizeString<Size>> {
    std::size_t operator()(
        const FarLib::FixedSizeString<Size>& str) const noexcept {
        return std::hash<std::string_view>{}(
            std::string_view(str.str.begin(), str.str.end()));
    }
};

#endif