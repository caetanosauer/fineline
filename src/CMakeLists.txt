INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_SOURCE_DIR}/foster-btree/src
  ${Boost_INCLUDE_DIRS}
)

set(fineline_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/fineline.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/options.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/threadlocal.cpp
)

add_library(fineline STATIC ${fineline_SRCS})
target_link_libraries(fineline
    legacy
    ${Boost_LIBRARIES}
    )
