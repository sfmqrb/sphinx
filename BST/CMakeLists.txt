add_library(BST INTERFACE)
target_include_directories(BST INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(BST INTERFACE project_compiler_flags)

# Define the BST_tests executable
add_executable(BST_tests BST_tests.cpp)

# Link the BST library and Catch2 to BST_tests
target_link_libraries(BST_tests PUBLIC BST)
target_link_libraries(BST_tests PRIVATE Catch2::Catch2WithMain)
catch_discover_tests(BST_tests)