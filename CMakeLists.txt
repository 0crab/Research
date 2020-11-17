cmake_minimum_required(VERSION 3.10)
project(Research)

set(CMAKE_CXX_STANDARD 14)

add_subdirectory(Concurrent_componet)

add_executable(Research main.cpp)