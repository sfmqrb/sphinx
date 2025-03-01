add_library(memento STATIC memento.hpp)
set_target_properties(memento PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(memento PUBLIC project_compiler_flags)
target_include_directories(memento INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
