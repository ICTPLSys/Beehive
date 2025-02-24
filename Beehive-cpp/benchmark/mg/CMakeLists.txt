cmake_minimum_required(VERSION 3.0)
project(mg)

add_executable(mg mg.cpp)

set(CMAKE_BUILD_TYPE "Release")

file(GLOB COMMON_SOURCES common/*.cpp)
add_library(common_objs OBJECT ${COMMON_SOURCES})


target_include_directories(mg PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(common_objs PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/common)


target_compile_options(mg PRIVATE -Wall -Wextra)
target_compile_options(common_objs PRIVATE -Wall -Wextra)


target_link_libraries(mg PRIVATE ${COMMON_LIBS} $<TARGET_OBJECTS:common_objs>)
