cmake_minimum_required(VERSION 3.10)
project(Research)

set(CMAKE_CXX_STANDARD 14)

link_libraries(pthread)

add_executable(table_test table_test.cpp )

add_executable(source_cuckoo_test source_cuckoo_test.cpp)

