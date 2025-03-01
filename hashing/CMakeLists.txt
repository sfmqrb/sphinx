add_library(hashing INTERFACE)
target_include_directories(hashing INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(hashing INTERFACE project_compiler_flags)

add_executable(hashing_tests hashing_tests.cpp)

target_link_libraries(hashing_tests PUBLIC hashing)
target_link_libraries(hashing_tests PRIVATE Catch2::Catch2WithMain)
catch_discover_tests(hashing_tests)