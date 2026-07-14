# CMake toolchain file for using `GCC8`
set(CMAKE_C_COMPILER gcc-8)
set(CMAKE_CXX_COMPILER g++-8)

# Statically link libstdc++ and libgcc. On a system whose default (dynamically
# linked) `libstdc++.so.6` was built by a much newer GCC than 8 (as is the
# case for the CI, which installs `gcc-8` via standalone `.deb` packages on
# top of a current Ubuntu release), some standard library internals whose ABI
# was not yet fully stable in GCC 8 -- in particular `std::filesystem::path`
# -- are laid out differently by GCC 8's headers than implemented by that
# newer shared library. Calling into the (newer) shared library's compiled
# `std::filesystem::path` destructor with a `this` pointer set up by GCC 8's
# (older) inlined constructor code corrupts memory and segfaults. Statically
# linking ensures both the inlined header code and the "shared library" code
# come from the exact same GCC 8.4.0 build, eliminating the mismatch.
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++ -static-libgcc")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libstdc++ -static-libgcc")
