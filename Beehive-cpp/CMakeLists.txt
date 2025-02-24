cmake_minimum_required(VERSION 3.25)
project(Beehive)

enable_language(CXX ASM)

find_package(Threads REQUIRED)
find_package(hdr_histogram REQUIRED)
find_package(Boost COMPONENTS program_options REQUIRED)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_BUILD_TYPE "Release")

include_directories(./include/)

set(SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
add_library(libcache STATIC ${SRC_DIR}/rdma/client.cpp ${SRC_DIR}/rdma/exchange_msg.cpp ${SRC_DIR}/cache/cache.cpp ${SRC_DIR}/utils/stats.cpp ${SRC_DIR}/utils/control.cpp ${SRC_DIR}/utils/uthreads.cpp)
add_library(libasync STATIC ${SRC_DIR}/async/xstack.s ${SRC_DIR}/async/async.cpp ${SRC_DIR}/utils/signal.cpp)
set(COMMON_LIBS libcache libasync ibverbs Threads::Threads ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_executable(server ${SRC_DIR}/rdma/server.cpp ${SRC_DIR}/rdma/exchange_msg.cpp)
target_link_libraries(server ibverbs Threads::Threads)

# libfibre, https://git.uwaterloo.ca/mkarsten/libfibre
set(LIBFIBRE_DIR "$ENV{LIBFIBRE_DIR}")
message(STATUS "LibFibre path: " ${LIBFIBRE_DIR})
find_library(LibFibre libfibre.so PATHS ${LIBFIBRE_DIR}/src REQUIRED)
include_directories(${LIBFIBRE_DIR}/src)
set(COMMON_LIBS ${COMMON_LIBS} ${LibFibre} papi)

add_subdirectory(test)
add_subdirectory(benchmark)
