cmake_minimum_required(VERSION 3.10)
project(Research)

set(CMAKE_CXX_STANDARD 14)

link_libraries(pthread)

add_executable(table_test table_test.cpp new_map.hh assert_msg.h kick_haza_pointer.h)

