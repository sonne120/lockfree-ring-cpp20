cmake_minimum_required(VERSION 3.20)
project(lockfree-ring-cpp20 VERSION 0.1.0 LANGUAGES CXX
        DESCRIPTION "Lock-free ring buffers in C++20: queues and the LMAX Disruptor")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(RING_BUILD_EXAMPLES "Build examples"  ON)
option(RING_BUILD_TESTS    "Build tests"     ON)
option(RING_BUILD_BENCH    "Build benchmark" ON)

find_package(Threads REQUIRED)

add_library(lockfree_ring INTERFACE)
add_library(ring::ring ALIAS lockfree_ring)
target_include_directories(lockfree_ring INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_link_libraries(lockfree_ring INTERFACE Threads::Threads)

function(ring_exe name src)
    add_executable(${name} ${src})
    target_link_libraries(${name} PRIVATE ring::ring)
    if(MSVC)
        target_compile_options(${name} PRIVATE /W4 /permissive-)
    else()
        target_compile_options(${name} PRIVATE -Wall -Wextra -Wpedantic -Wshadow)
    endif()
endfunction()

if(RING_BUILD_EXAMPLES)
    ring_exe(queue_basics examples/queue_basics.cpp)
    ring_exe(one_to_one  examples/one_to_one.cpp)
    ring_exe(one_to_many examples/one_to_many.cpp)
    ring_exe(pipeline    examples/pipeline.cpp)
endif()

if(RING_BUILD_TESTS)
    enable_testing()
    ring_exe(queue_tests tests/queue_tests.cpp)
    add_test(NAME queue_tests COMMAND queue_tests)
    ring_exe(disruptor_tests tests/disruptor_tests.cpp)
    add_test(NAME disruptor_tests COMMAND disruptor_tests)
    if(RING_BUILD_EXAMPLES)
        add_test(NAME pipeline_is_deterministic COMMAND pipeline)
    endif()
endif()

if(RING_BUILD_BENCH)
    ring_exe(queue_vs_disruptor bench/queue_vs_disruptor.cpp)
endif()
