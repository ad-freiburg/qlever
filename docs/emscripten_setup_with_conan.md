# Compiling the qlever code to WebAssembly using Emscripten and the Conan package manager

## Installation

First, install the packages needed for building the binaries listed in the Dockerfile.

Then, follow the instructions on how to install Conan respectively.
(We don't need to install Emscripten/emsdk because we are using the one from Conan.)

## Conan profiles

For Conan, we need 2 conan profiles: a build profile and a host profile.
The host profile is `emscripten.profile` in the folder `conanprofiles`. Copy it to the profiles folder of your conan installation ( [CONAN_HOME]/profiles ).
To get a build profile, run `conan profile detect`.

## Directories

Now, create a `qlever` directory where we store and build the files.
Copy: `src`, `test`, `e2e`, `benchmark`, `.git`, `CMakelists.txt` and `CompilationInfo.cmake` from qlever
Also copy the `conanfile.txt` to `qlever` dir. 

## Build commands

#### in qlever dir:
run `conan install . -pr:b default -pr:h emscripten.profile -s build_type=Release -b missing -of build`

#### in build dir:
run `cmake -S .. -B . -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release -DLOGLEVEL=INFO -DUSE_PARALLEL=true -D_NO_TIMING_TESTS=ON -D_DISABLE_EMSCRIPTEN_PROBLEMATIC_TESTS=ON -D_DISABLE_EMSCRIPTEN_TARGETS=ON -GNinja`

and finally, run: `cmake --build .`
