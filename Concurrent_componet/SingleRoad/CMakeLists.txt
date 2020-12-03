cmake_minimum_required(VERSION 3.10)
project(Research)

set(CMAKE_CXX_STANDARD 11)

link_libraries(pthread atomic)

add_executable(SingleRoadTest SingleRoadTest.cpp)
