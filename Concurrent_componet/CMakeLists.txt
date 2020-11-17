cmake_minimum_required(VERSION 3.10)
project(Research)

set(CMAKE_CXX_STANDARD 14)

link_libraries(pthread)

add_executable(MSQueueTest MichaelScottQueueTest.cpp)

add_executable(kv_rw kv_rw.cpp)