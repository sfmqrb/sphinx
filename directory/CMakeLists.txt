add_library(directory INTERFACE)
target_include_directories(directory INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(directory INTERFACE project_compiler_flags)

add_executable(directory_tests directory_tests.cpp)

target_link_libraries(directory_tests
    PUBLIC
        directory
    PRIVATE
        Catch2::Catch2WithMain
        block
        hashtable
        SSDLog
        extension_block
        segment
        buffer_pool2 
)

catch_discover_tests(directory_tests)

