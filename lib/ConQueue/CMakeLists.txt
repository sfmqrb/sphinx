add_library(concurrentqueue STATIC concurrentqueue.h)
set_target_properties(concurrentqueue PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(concurrentqueue PUBLIC project_compiler_flags)
target_include_directories(concurrentqueue INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
