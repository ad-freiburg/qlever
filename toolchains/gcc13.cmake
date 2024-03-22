# Toolchain that uses G++ 13
set(CMAKE_C_COMPILER gcc-13)
set(CMAKE_CXX_COMPILER g++-13)
# Disable errors for known false-positive warnings
set(CMAKE_CXX_FLAGS "-Wno-error=array-bounds -Wno-error=nonnull -Wno-error=maybe-uninitialized -Wno-error=dangling-reference")
