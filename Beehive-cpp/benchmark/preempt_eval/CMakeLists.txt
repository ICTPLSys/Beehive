add_executable(preempt_eval preempt_eval.cpp)
target_link_libraries(preempt_eval ${COMMON_LIBS} hdr_histogram_static ssl crypto)

