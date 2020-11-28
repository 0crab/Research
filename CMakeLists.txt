cmake_minimum_required(VERSION 3.10)
project(Research)

set(CMAKE_CXX_STANDARD 14)

add_subdirectory(Concurrent_componet/MSQ)

add_subdirectory(Concurrent_componet/StoreBased)

add_subdirectory(Concurrent_componet/MultiRoad)

add_subdirectory(Concurrent_componet/SingleRoad)

add_subdirectory(Concurrent_componet/CAS_TEST)

add_executable(Research main.cpp)