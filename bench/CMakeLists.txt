INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/bench
)

set(ycsb_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/ycsb.cpp
)

add_executable(ycsb ${ycsb_SRCS})

# target_link_libraries(ycsb
#     )
