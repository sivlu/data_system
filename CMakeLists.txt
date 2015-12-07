cmake_minimum_required(VERSION 3.3)
project(blus_cs165_2015_base)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

INCLUDE_DIRECTORIES(src/include/)

set(SOURCE_FILES
    src/include/common.h
    src/include/cs165_api.h
    src/include/message.h
    src/include/utils.h
    src/client.c
    src/Makefile
    src/README.md
    src/server.c
    src/utils.c src/include/parser.h src/cs165_api.c src/cs165_api.c.h src/include/data_structure.h src/include/hashtable.c src/include/hashtable.h)


add_executable(blus_cs165_2015_base ${SOURCE_FILES})