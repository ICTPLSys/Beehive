cmake_minimum_required(VERSION 3.25)
project(Cfibre)

enable_language(CXX C)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_BUILD_TYPE "Release")

set(LIBFIBRE_DIR "$ENV{LIBFIBRE_DIR}")
message(STATUS "LibFibre path: " ${LIBFIBRE_DIR})
find_library(LibFibre libfibre.so PATHS ${LIBFIBRE_DIR}/src REQUIRED)
include_directories(${LIBFIBRE_DIR}/src ./)

add_library(cfibre STATIC cfibre.cpp cfibre.h)
target_link_libraries(cfibre ${LibFibre})
set(FIBRELIB cfibre)

add_executable(test test.cpp)
target_link_libraries(test ${LibFibre} ${FIBRELIB})
