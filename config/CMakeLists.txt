add_library(config INTERFACE)
target_include_directories(config INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(config INTERFACE project_compiler_flags)