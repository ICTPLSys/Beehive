add_executable(yield_overhead yield_overhead.cpp)
target_link_libraries(yield_overhead ${COMMON_LIBS})

add_executable(context_switch_overhead context_switch_overhead.cpp)
target_link_libraries(context_switch_overhead ${COMMON_LIBS} papi)

add_executable(copy_selected copy_selected.cpp)
target_link_libraries(copy_selected ${COMMON_LIBS})

add_executable(traverse_array traverse_array.cpp)
target_link_libraries(traverse_array ${COMMON_LIBS})
