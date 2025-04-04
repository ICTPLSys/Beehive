add_executable(kvs_service_throughput throughput.cpp)
target_link_libraries(kvs_service_throughput ${COMMON_LIBS} hdr_histogram_static)

add_executable(kvs_service_latency latency.cpp)
target_link_libraries(kvs_service_latency ${COMMON_LIBS} hdr_histogram_static)
