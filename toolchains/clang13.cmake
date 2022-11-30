# CMake toolchain file for using `clang++-13`
set(CMAKE_C_COMPILER clang-13)
set(CMAKE_CXX_COMPILER clang++-13)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
