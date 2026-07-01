# CMake toolchain file for using `clang++-22`
set(CMAKE_C_COMPILER clang-22)
set(CMAKE_CXX_COMPILER clang++-22)
# See https://github.com/llvm/llvm-project/issues/76515
set(CMAKE_CXX_FLAGS "-Wno-deprecated-declarations")
