cmake_minimum_required(VERSION 3.10)
project(BenchmarkProject)

# Specify the C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Add executables
add_executable(benchmark_read_latency2 benchmark_read_latency2.cpp ${SOURCE_FILES})
add_executable(benchmark_multi_threaded benchmark_multi_threaded.cpp ${SOURCE_FILES})
add_executable(benchmark_main benchmark_main.cpp ${SOURCE_FILES})
add_executable(benchmark_mixed benchmark_mixed.cpp ${SOURCE_FILES})
add_executable(benchmark_xdp benchmark_xdp.cpp ${SOURCE_FILES})
add_executable(benchmark_load_factor_query_insert benchmark_load_factor_query_insert.cpp ${SOURCE_FILES})
add_executable(benchmark_extra_bits benchmark_extra_bits.cpp ${SOURCE_FILES})
add_executable(benchmark_tail benchmark_tail.cpp ${SOURCE_FILES})

target_link_libraries(benchmark_read_latency2
        PRIVATE
        directory
        block
        hashtable
        SSDLog
        extension_block
        segment
)
target_link_libraries(benchmark_multi_threaded
        PRIVATE
        directory
        block
        hashtable
        SSDLog
        extension_block
        segment
)
target_link_libraries(benchmark_main
        PRIVATE
        directory
        block
        hashtable
        SSDLog
        extension_block
        segment
)
target_link_libraries(benchmark_mixed
        PRIVATE
        directory
        block
        hashtable
        SSDLog
        extension_block
        segment
)
target_link_libraries(benchmark_xdp
        PRIVATE
        directory
        block
        hashtable
        SSDLog
        extension_block
        segment
        xdp
)
target_link_libraries(benchmark_extra_bits
        PRIVATE
        directory
        block
        hashtable
        SSDLog
        extension_block
        segment
)
target_link_libraries(benchmark_tail
        PRIVATE
        directory
        block
        hashtable
        SSDLog
        extension_block
        segment
)
target_link_libraries(benchmark_load_factor_query_insert
        PRIVATE
        directory
        block
        hashtable
        SSDLog
        extension_block
        segment
)
# Add subdirectory for the recorder
add_subdirectory(recorder)
