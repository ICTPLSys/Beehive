add_library(libgraph STATIC graph.cpp)

add_executable(bfs bfs.cpp)
target_link_libraries(bfs libgraph ${COMMON_LIBS} papi)

add_executable(page_rank page_rank.cpp)
target_link_libraries(page_rank libgraph ${COMMON_LIBS} papi)

add_executable(cc cc.cpp)
target_link_libraries(cc libgraph ${COMMON_LIBS} papi)

add_executable(bc bc.cpp)
target_link_libraries(bc libgraph ${COMMON_LIBS} papi)
