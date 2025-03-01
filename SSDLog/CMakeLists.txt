add_library(SSDLog INTERFACE)
target_include_directories(SSDLog INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(SSDLog INTERFACE project_compiler_flags)

add_executable(SSDLog_tests SSDLog_tests.cpp)

target_link_libraries(SSDLog_tests PUBLIC SSDLog)
target_link_libraries(SSDLog_tests PRIVATE Catch2::Catch2WithMain)
catch_discover_tests(SSDLog_tests)