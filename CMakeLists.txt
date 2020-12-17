cmake_minimum_required(VERSION 3.10)
project(Research)

set(CMAKE_CXX_STANDARD 14)

add_subdirectory(Concurrent_componet/MSQ)

add_subdirectory(Concurrent_componet/SingleRoad)

add_subdirectory(Concurrent_componet/RWlock)

add_subdirectory(Concurrent_componet/VersionControl)

add_subdirectory(Concurrent_componet/HazaPointer)

add_subdirectory(Cuckoo_improve/libcuckoo_test)

add_subdirectory(Cuckoo_improve/improve_test)

add_executable(tmptest tmptest.cpp tmptest.cpp)