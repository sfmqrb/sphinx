add_library(xxhash STATIC xxhash64.h)
set_target_properties(xxhash PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(xxhash PUBLIC project_compiler_flags)
target_include_directories(xxhash INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
