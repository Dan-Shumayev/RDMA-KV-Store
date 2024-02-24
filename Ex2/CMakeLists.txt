cmake_minimum_required(VERSION 3.10)
project(Ex2 C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -libverbs")

add_executable(Ex2
        bw_template.c
        #onemsg.c
        )
