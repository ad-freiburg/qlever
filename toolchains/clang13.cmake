# CMake toolchain file for using `clang++-13` together with `libc++`
set(CMAKE_C_COMPILER clang-13)
set(CMAKE_CXX_COMPILER clang++-13)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lc++abi")
