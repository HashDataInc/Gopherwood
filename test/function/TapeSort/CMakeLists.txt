cmake_minimum_required(VERSION 2.8)
project(DBMS_Implementation)

#set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        src/bufferOps.h
        src/dbtproj.h
        src/fileOps.cpp
        src/fileOps.h
        src/main.cpp
        src/MergeSort.cpp
        src/recordOps.cpp
        src/recordOps.h
        src/recordPtr.cpp
        src/recordPtr.h
        src/sortBuffer.cpp
        src/gopherwood.h
        src/sortBuffer.h src/dbtproj.cpp)

add_executable(DBMS_Implementation ${SOURCE_FILES})
target_link_libraries(DBMS_Implementation gopherwood)