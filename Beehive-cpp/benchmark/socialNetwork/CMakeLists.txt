find_package(cpp-jwt REQUIRED)

add_executable(
    socialNetwork
    latency_main.cpp
    utils.cpp
    SimpleBackEndServer.cpp
    BackEndService.cpp
    states.cpp
)


target_include_directories(
    socialNetwork PRIVATE
)

target_link_libraries(
    socialNetwork
    PRIVATE libpthread.so
    PRIVATE cpp-jwt::cpp-jwt
    hdr_histogram
    ${COMMON_LIBS}
)
