cmake_minimum_required(VERSION 3.10)
project(Research)

set(CMAKE_CXX_STANDARD 14)

link_libraries(pthread)

add_executable(VersionControl VersionControl.cpp)
