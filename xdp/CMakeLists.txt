add_library(xdp INTERFACE)
target_include_directories(xdp INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(xdp INTERFACE project_compiler_flags)

add_executable(xdp_tests xdp_tests.cpp)

target_link_libraries(xdp_tests
    PUBLIC
        xdp 
    PRIVATE
        Catch2::Catch2WithMain
        directory
        block
        hashtable
        SSDLog
        extension_block
        segment
        buffer_pool2 
)

catch_discover_tests(xdp_tests)

