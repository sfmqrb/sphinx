add_library(payload INTERFACE)
target_include_directories(payload INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(payload INTERFACE project_compiler_flags)

add_executable(payload_tests payload_tests.cpp)

target_link_libraries(payload_tests PUBLIC payload)
target_link_libraries(payload_tests PRIVATE Catch2::Catch2WithMain)
catch_discover_tests(payload_tests)