#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <boost/timer/progress_display.hpp>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <string_view>

inline const char *skip(const char *begin, const char *end, char sep) {
    for (const char *p = begin; p < end; p++) {
        if (*p == sep) return p + 1;
    }
    return end;
}

template <size_t N>
struct ShortString {
    std::array<char, N> value;

    ShortString() = default;
    ShortString(const ShortString &) = default;
    ShortString(ShortString &&) = default;
    ShortString(std::string_view name) {
        if (name.length() < N) {
            std::memcpy(value.data(), name.data(), name.length());
            std::memset(value.data() + name.length(), 0, N - name.length());
        } else {
            std::memcpy(value.data(), name.data(), N);
        }
    }

    constexpr ShortString &operator=(const ShortString &) = default;
    constexpr ShortString &operator=(ShortString &&) = default;

    std::string_view as_string_view() const {
        return std::string_view(value.data(), N);
    }

    bool operator==(const ShortString &other) const {
        return value == other.value;
    }

    bool operator<(const ShortString &other) const {
        return as_string_view() < other.as_string_view();
    }
};

template <>
struct ShortString<8> {
    std::array<char, 8> value;

    ShortString() = default;
    ShortString(const ShortString &) = default;
    ShortString(ShortString &&) = default;
    ShortString(std::string_view name) {
        if (name.length() < 8) {
            std::memcpy(value.data(), name.data(), name.length());
            std::memset(value.data() + name.length(), 0, 8 - name.length());
        } else {
            std::memcpy(value.data(), name.data(), 8);
        }
    }

    constexpr ShortString &operator=(const ShortString &) = default;
    constexpr ShortString &operator=(ShortString &&) = default;

    std::string_view as_string_view() const {
        return std::string_view(value.data(), 8);
    }

    bool operator==(const ShortString &other) const {
        return *reinterpret_cast<const int64_t *>(value.data()) ==
               *reinterpret_cast<const int64_t *>(other.value.data());
    }

    bool operator<(const ShortString &other) const {
        return *reinterpret_cast<const int64_t *>(value.data()) <
               *reinterpret_cast<const int64_t *>(other.value.data());
    }
};

template <size_t N>
struct std::hash<ShortString<N>> {
    std::size_t operator()(const ShortString<N> &s) const noexcept {
        return std::hash<std::string_view>{}(s.as_string_view());
    }
};

template <>
struct std::hash<ShortString<8>> {
    std::size_t operator()(const ShortString<8> &s) const noexcept {
        return *reinterpret_cast<const size_t *>(s.value.data());
    }
};

template <typename Fn>
inline void read_csv_buffer(const char *buffer, size_t size,
                            Fn &&process_line) {
    const char *buffer_end = buffer + size;

    // skip head
    const char *p = skip(buffer, buffer_end, '\n');

    boost::timer::progress_display pd(buffer_end - p);
    size_t line_idx = 0;
    while (p < buffer_end) {
        const char *next_line = skip(p, buffer_end, '\n');
        process_line(std::string_view(p, next_line - p));
        pd += next_line - p;
        p = next_line;
        line_idx++;
    }
    std::cout << "read lines: " << line_idx << std::endl;
}

// https://www.kaggle.com/datasets/bulter22/airline-data
// "/path/to/data/airline/airline.csv.shuffle"
template <typename Fn>
bool read_csv_file(const char *file_name, Fn &&process_line) {
    // Open the file
    int fd = open(file_name, O_RDONLY);
    if (fd == -1) {
        std::cerr << "Error opening file: " << std::strerror(errno)
                  << std::endl;
        return false;
    }

    // Get the file size
    struct stat file_info;
    if (fstat(fd, &file_info) == -1) {
        std::cerr << "Error getting file size: " << std::strerror(errno)
                  << std::endl;
        close(fd);
        return false;
    }
    size_t file_size = file_info.st_size;

    void *mapped = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        std::cerr << "Error mapping file: " << std::strerror(errno)
                  << std::endl;
        close(fd);
        return false;
    }
    close(fd);
    read_csv_buffer(static_cast<const char *>(mapped), file_size, process_line);
    if (munmap(mapped, file_size) == -1) {
        std::cerr << "Error unmapping file: " << std::strerror(errno)
                  << std::endl;
        return false;
    }
    return true;
}
