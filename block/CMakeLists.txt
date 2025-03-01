add_library(block INTERFACE)
target_include_directories(block INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(block INTERFACE project_compiler_flags)

# Define the block_tests executable
add_executable(block_tests block_tests.cpp)
target_link_libraries(block_tests PRIVATE block Catch2::Catch2WithMain hashtable SSDLog)
# Link the block library and Catch2 to block_tests
target_link_libraries(block_tests PUBLIC block)
target_link_libraries(block_tests PRIVATE Catch2::Catch2WithMain)
catch_discover_tests(block_tests)