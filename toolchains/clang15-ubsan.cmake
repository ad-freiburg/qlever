# CMake toolchain file for using `clang++-14` together with `libstdc++`
set(CMAKE_C_COMPILER clang-14)
set(CMAKE_CXX_COMPILER clang++-14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
#set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lc++abi")
