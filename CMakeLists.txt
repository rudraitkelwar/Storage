cmake_minimum_required(VERSION 3.16)
project(mini_blockstore CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(common
  common/net.cpp
  common/crc32.cpp
)

target_include_directories(common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(block_server
  server/main.cpp
  server/block_store.cpp
)

target_link_libraries(block_server PRIVATE common)

add_executable(block_client
  client/main.cpp
)

target_link_libraries(block_client PRIVATE common)
