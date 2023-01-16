# Toolchain that uses G++ 12
set(CMAKE_C_COMPILER gcc-12)
set(CMAKE_CXX_COMPILER clang++-15)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Og -g -fprofile-instr-generate -fcoverage-mapping")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
set(PROFDATA_TOOL "llvm-profdata-15")
set(PROFGEN_TOOL "llvm-profgen-15")

