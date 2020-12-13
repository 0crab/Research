cmake_minimum_required(VERSION 3.10)
project(Research)

set(CMAKE_CXX_STANDARD 14)

link_libraries(pthread)

add_executable(mico_test mico_test.cpp)

add_executable(ycsb_tes ycsb_test.cpp)

add_executable(CH_HybridTest CH_HybridTest.cpp)

add_executable(CH_YCSBTest CH_YCSBTest.cpp)