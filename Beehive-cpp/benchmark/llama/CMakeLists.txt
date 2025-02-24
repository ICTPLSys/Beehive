add_executable(run_chat_far run_chat_far.cpp)
target_link_libraries(run_chat_far ${COMMON_LIBS} m)

add_executable(run_chat_far_prof run_chat_far_prof.cpp)
target_link_libraries(run_chat_far_prof ${COMMON_LIBS} m)