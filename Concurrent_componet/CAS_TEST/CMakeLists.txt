cmake_minimum_required(VERSION 3.17)
project(locks)

link_libraries(pthread)

add_executable(cas_test cas_test.cpp)