cmake_minimum_required(VERSION 3.3)
project(blus_cs165_2015_base)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    src/include/common.h
    src/include/cs165_api.h
    src/include/message.h
    src/include/utils.h
    src/client.c
    src/Makefile
    src/README.md
    src/server.c
    src/utils.c)

add_executable(blus_cs165_2015_base ${SOURCE_FILES})