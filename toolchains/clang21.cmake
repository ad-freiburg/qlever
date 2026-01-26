# CMake toolchain file for using `clang++-21`
set(CMAKE_C_COMPILER clang-21)
set(CMAKE_CXX_COMPILER clang++-21)
# See https://github.com/llvm/llvm-project/issues/76515
set(CMAKE_CXX_FLAGS "-Wno-deprecated-declarations")
