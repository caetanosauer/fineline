set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

find_package(SQLite3 REQUIRED)

INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/foster-btree/src
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_SOURCE_DIR}/src/legacy
  ${SQLITE3_INCLUDE_DIRS}
  ${Boost_INCLUDE_DIRS}
)

set(legacy_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/lsn.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/carray.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/log_file.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/log_storage.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/log_index_sqlite.cpp
)

add_library(legacy STATIC ${legacy_SRCS})

set(LIBS ${SQLITE3_LIBRARIES} ${Boost_LIBRARIES})

target_link_libraries(legacy ${LIBS})
