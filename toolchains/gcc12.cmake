# Toolchain that uses G++ 12
set(CMAKE_C_COMPILER gcc-12)
set(CMAKE_CXX_COMPILER g++-12)
# Disable errors for known false-positive warnings
set(CMAKE_CXX_FLAGS "-Wno-error=array-bounds -Wno-error=restrict")
