INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_SOURCE_DIR}/foster-btree/src
  ${CMAKE_SOURCE_DIR}/bench
)

set(ycsb_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/ycsb.cpp
)
add_executable(ycsb ${ycsb_SRCS})
target_link_libraries(ycsb fineline)

set(loginspect_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/loginspect.cpp
)
add_executable(loginspect ${loginspect_SRCS})
target_link_libraries(loginspect fineline)
