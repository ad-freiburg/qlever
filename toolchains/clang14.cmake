# CMake toolchain file for using `clang++-14`
set(CMAKE_C_COMPILER clang-14)
set(CMAKE_CXX_COMPILER clang++-14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
