enable_testing()

add_executable(test_rdma test_rdma.cpp ${SRC_DIR}/rdma/client.cpp ${SRC_DIR}/rdma/exchange_msg.cpp ${SRC_DIR}/utils/stats.cpp)
target_link_libraries(test_rdma ibverbs ${COMMON_LIBS})
add_test(NAME TestRDMA COMMAND test_rdma)

add_executable(test_local_cache test_local_cache.cpp)
target_link_libraries(test_local_cache ${COMMON_LIBS})
add_test(NAME TestLocalCache COMMAND test_local_cache)

add_executable(test_vector test_vector.cpp)
target_link_libraries(test_vector ${COMMON_LIBS})
add_test(NAME TestVector COMMAND test_vector)

# add_executable(test_bplus_tree_map test_bplus_tree_map.cpp)
# target_link_libraries(test_bplus_tree_map ${COMMON_LIBS})
# add_test(NAME TestBPlusTreeMap COMMAND test_bplus_tree_map)

add_executable(test_hopscotch test_hopscotch.cpp)
target_link_libraries(test_hopscotch ${COMMON_LIBS})
add_test(NAME TestHopscotch COMMAND test_hopscotch)

add_executable(test_concurrent_allocate test_concurrent_allocate.cpp)
target_link_libraries(test_concurrent_allocate ${COMMON_LIBS})
add_test(NAME TestConcurrentAllocate COMMAND test_concurrent_allocate)

add_executable(test_concurrent_rdma test_concurrent_rdma.cpp)
target_link_libraries(test_concurrent_rdma ${COMMON_LIBS})
add_test(NAME TestConcurrentRDMA COMMAND test_concurrent_rdma)

add_executable(test_concurrent_cache test_concurrent_cache.cpp)
target_link_libraries(test_concurrent_cache ${COMMON_LIBS})
add_test(NAME TestConcurrentCache COMMAND test_concurrent_cache)

# add_executable(test_sync test_sync.cpp)
# target_link_libraries(test_sync ${COMMON_LIBS})
# add_test(NAME TestSync COMMAND test_sync)

# add_executable(test_sync_simple test_sync_simple.cpp)
# target_link_libraries(test_sync_simple ${COMMON_LIBS})
# add_test(NAME TestSyncSimple COMMAND test_sync_simple)

add_executable(test_deref_scope test_deref_scope.cpp)
target_link_libraries(test_deref_scope ${COMMON_LIBS})
add_test(NAME TestDerefScope COMMAND test_deref_scope)

add_executable(test_region_remote_allocator test_region_remote_allocator.cpp)
target_link_libraries(test_region_remote_allocator ${COMMON_LIBS})
add_test(NAME TestRegionRemoteAllocator COMMAND test_region_remote_allocator)

# add_executable(test_far_vector test_far_vector.cpp)
# target_link_libraries(test_far_vector ${COMMON_LIBS})
# add_test(NAME TestFarVector COMMAND test_far_vector)

add_executable(test_fence test_fence.cpp)
target_link_libraries(test_fence ${COMMON_LIBS})
add_test(NAME TestFence COMMAND test_fence)

add_executable(test_small_vec test_small_vec.cpp)
target_link_libraries(test_small_vec ${COMMON_LIBS})
add_test(NAME TestSmallVec COMMAND test_small_vec)

add_executable(test_concurrent_flat_map test_concurrent_flat_map.cpp)
target_link_libraries(test_concurrent_flat_map ${COMMON_LIBS})
add_test(NAME TestConcurrentFlatMap COMMAND test_concurrent_flat_map)

add_executable(test_generic_stream test_generic_stream.cpp)
target_link_libraries(test_generic_stream ${COMMON_LIBS})
add_test(NAME TestGenericStream COMMAND test_generic_stream)

add_executable(test_btree_string_multimap test_btree_string_multimap.cpp)
target_link_libraries(test_btree_string_multimap ${COMMON_LIBS})
add_test(NAME TestBTreeStringMultiMap COMMAND test_btree_string_multimap)

add_executable(test_view test_view.cpp)
target_link_libraries(test_view ${COMMON_LIBS})
add_test(NAME TestView COMMAND test_view)

add_executable(test_ordered_vec test_ordered_vec.cpp)
target_link_libraries(test_ordered_vec ${COMMON_LIBS})
add_test(NAME TestOrderedVec COMMAND test_ordered_vec)
