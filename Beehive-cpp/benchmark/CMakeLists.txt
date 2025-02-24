add_executable(bplus_tree_iteration bplus_tree_iteration.cpp)
target_link_libraries(bplus_tree_iteration ${COMMON_LIBS})

add_executable(kmeans kmeans.cpp)
target_link_libraries(kmeans ${COMMON_LIBS} papi)

add_executable(pointer_chasing pointer_chasing.cpp)
target_link_libraries(pointer_chasing ${COMMON_LIBS})

add_executable(hashtable_multi_find hashtable_multi_find.cpp)
target_link_libraries(hashtable_multi_find ${COMMON_LIBS} boost_coroutine boost_context)

add_executable(hashtable_find_profile hashtable_find_profile.cpp)
target_link_libraries(hashtable_find_profile ${COMMON_LIBS})

add_executable(hopscotch_multi_find hopscotch_multi_find.cpp)
target_link_libraries(hopscotch_multi_find ${COMMON_LIBS})

add_executable(hopscotch_group_zipfian hopscotch_group_zipfian.cpp)
target_link_libraries(hopscotch_group_zipfian ${COMMON_LIBS})

add_executable(hopscotch_latency hopscotch_latency.cpp)
target_link_libraries(hopscotch_latency ${COMMON_LIBS} hdr_histogram_static)
add_executable(concurrent_kv_service concurrent_kv_service.cpp)
target_link_libraries(concurrent_kv_service ${COMMON_LIBS} hdr_histogram_static)

add_executable(vector_sort vector_sort.cpp)
target_link_libraries(vector_sort ${COMMON_LIBS})
add_subdirectory(microbenchmarks)
add_subdirectory(graph)
add_subdirectory(DataFrame)
add_subdirectory(llama)
# add_subdirectory(mapreduce)
add_subdirectory(mg)
add_subdirectory(socialNetwork)
