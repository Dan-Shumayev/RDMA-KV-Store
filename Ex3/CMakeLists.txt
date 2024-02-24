cmake_minimum_required(VERSION 3.13)
project(Ex3 C)

set(CMAKE_C_STANDARD 99)

add_executable(Ex3 main.c kv_store.cpp bw_template.c kv_store.h)
