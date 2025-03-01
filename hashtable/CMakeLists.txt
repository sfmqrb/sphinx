add_library(hashtable STATIC hashtable.cpp)
target_link_libraries(hashtable PUBLIC project_compiler_flags)
target_include_directories(hashtable INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

# Setup DB tests
add_executable(hashtable_tests hashtable_tests.cpp)
target_link_libraries(hashtable_tests PUBLIC hashtable)
target_link_libraries(hashtable_tests PRIVATE Catch2::Catch2WithMain)

catch_discover_tests(hashtable_tests)
