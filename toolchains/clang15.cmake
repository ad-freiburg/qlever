# CMake toolchain file for using `clang++-15`
set(CMAKE_C_COMPILER clang-15)
set(CMAKE_CXX_COMPILER clang++-15)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++ -lc++abi")
