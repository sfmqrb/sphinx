add_library(segment INTERFACE)
target_include_directories(segment INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(segment INTERFACE project_compiler_flags)

add_executable(segment_tests segment_tests.cpp)

target_link_libraries(segment_tests PUBLIC segment)
target_link_libraries(segment_tests PRIVATE Catch2::Catch2WithMain block hashtable SSDLog extension_block)

catch_discover_tests(segment_tests)