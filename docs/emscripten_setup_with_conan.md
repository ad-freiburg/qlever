# Compiling the qlever code to WebAssembly using Emscripten and the Conan package manager

## Installation

Install the packages needed for building the binaries listed in the Dockerfile.

Then, follow the instructions on how to install Conan respectively.
We don't need to install Emscripten/emsdk because we are using the one from Conan.

## Repository

Checkout the repository with

```bash
git clone --recursive https://github.com/ad-freiburg/qlever
```
The --recursive is important because the QLever repository uses a few submodules.

## Conan profiles

Because we are cross-compiling with Emscripten, we need 2 conan profiles: a build profile and a host profile.
The host profile is `emscripten.profile` in the folder `conanprofiles`. Copy it to the profiles folder of your conan installation ( [CONAN_HOME]/profiles ).
Now we need a build profile. If you are new to conan and don't have your own build profile(s) already, you can simply get one by running `conan profile detect`.
This will give you a default build profile based on your OS.
It's only an estimation of an appropriate build profile and can be customized later on, but it is sufficient for our purposes.
For more information see [conan profiles](https://docs.conan.io/2/reference/config_files/profiles.html).

## Build commands

```bash
conan install . -pr:b default -pr:h emscripten.profile -s build_type=Release -b missing -of build
cd build
cmake -S .. -B . -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release -DLOGLEVEL=INFO -DUSE_PARALLEL=true -D_NO_TIMING_TESTS=ON -D_EMSCRIPTEN_NO_INDEXBUILDER_AND_SERVER=ON -D_DISABLE_EMSCRIPTEN_PROBLEMATIC_TESTS=ON -GNinja
cmake --build .
```
