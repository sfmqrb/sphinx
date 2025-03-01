cmake_minimum_required(VERSION 3.15)
# set(CMAKE_C_COMPILER /usr/local/gcc-14.1.0/bin/gcc-14.1.0)
# set(CMAKE_CXX_COMPILER /usr/local/gcc-14.1.0/bin/g++-14.1.0)
project(DHT VERSION 0.1)

# Specify the path to the desired GCC and G++ compilers
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

# RPATH settings
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_BUILD_RPATH /usr/local/gcc-14.1.0/lib64)

# Compiler flags interface
add_library(project_compiler_flags INTERFACE)
set_target_properties(project_compiler_flags PROPERTIES LINKER_LANGUAGE CXX)

# Add the GCC and G++ compiler settings to the interface
target_compile_options(project_compiler_flags INTERFACE
    $<$<COMPILE_LANG_AND_ID:CXX,GNU>: -std=c++23>
    $<$<COMPILE_LANG_AND_ID:CXX,GNU>: -mbmi2 -march=native>
)
# RPATH settings to the interface
target_link_libraries(project_compiler_flags INTERFACE
    "-Wl,-rpath,/usr/local/gcc-14.1.0/lib64,-latomic"
)
# Generate CLangD's LSP config
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# add compiler warning flags just when building this project via
# the BUILD_INTERFACE genex
set(gcc_like_cxx "$<COMPILE_LANG_AND_ID:CXX,ARMClang,AppleClang,Clang,GNU,LCC>")
set(msvc_cxx "$<COMPILE_LANG_AND_ID:CXX,MSVC>")
target_compile_options(project_compiler_flags INTERFACE
        "$<${gcc_like_cxx}:$<BUILD_INTERFACE:-Wall;-Wextra;-Winline;-Wshadow;-Wformat=2;-Wunused;-O3>>"
        "$<${msvc_cxx}:$<BUILD_INTERFACE:-W3>>"
)
#         "$<${gcc_like_cxx}:$<BUILD_INTERFACE:-Wall;-Wextra;-Winline;-Wshadow;-Wformat=2;-Wunused;-O3>>"
add_compile_options(-mbmi2 -march=native)

# Setup Catch2, our unit testing framework
add_subdirectory(lib/Catch2)

# Setup xxhash
add_subdirectory(lib/xxhash)
add_subdirectory(lib/ConQueue)
add_subdirectory(lib/memento)

# Enable testing
include(CTest)
include(Catch)

add_subdirectory(config)
add_subdirectory(block)
add_subdirectory(SSDLog)
add_subdirectory(bitset_wrapper)
add_subdirectory(payload)
add_subdirectory(benchmark)
add_subdirectory(hashtable)
add_subdirectory(BST)
add_subdirectory(hashing)
add_subdirectory(extension_block)
add_subdirectory(directory)
add_subdirectory(fingerprint_gen_helper)
add_subdirectory(segment)
add_subdirectory(buffer_pool2)
