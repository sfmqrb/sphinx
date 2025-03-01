add_library(recorder INTERFACE)
target_include_directories(recorder INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(recorder INTERFACE project_compiler_flags)

add_executable(recorder_tests recorder_tests.cpp)

target_link_libraries(recorder_tests PUBLIC recorder)
target_link_libraries(recorder_tests PRIVATE Catch2::Catch2WithMain)
catch_discover_tests(recorder_tests)
