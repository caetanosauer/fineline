INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/foster-btree/src
  ${CMAKE_SOURCE_DIR}/src/legacy
)

set(legacy_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/lsn.cpp
)

add_library(legacy STATIC ${legacy_SRCS})

# target_link_libraries(fineline
#     lib1
#     lib2
#     )

