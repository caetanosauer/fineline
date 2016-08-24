INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_SOURCE_DIR}/foster-btree/src
)

set(fineline_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/txncontext.cpp
)

add_library(fineline STATIC ${fineline_SRCS})
# target_link_libraries(fineline
#     lib1
#     lib2
#     )
