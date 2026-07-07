[settings]
arch=wasm64
build_type=Release
compiler=emcc
compiler.cppstd=20
compiler.libcxx=libc++
# Keep this in sync with the EMSDK_VERSION in
# .github/workflows/native-build-with-conan-and-emscripten.yml. The Conan
# Center emsdk recipe lags behind upstream, so emsdk is installed directly
# (CI uses mymindstorm/setup-emsdk; locally, install emsdk yourself and
# export EMSDK before running conan).
compiler.version=6.0.2
os=Emscripten
compiler.threads=posix

[options]
icu/*:with_icuio=False
# `static` compiles the ~34MB ICU data into libicudata.a. With `archive` (the
# recipe default) the data lives in an external res/icudt*.dat file that never
# gets embedded into the wasm, so ICU fails at runtime with `U_FILE_ACCESS_ERROR`.
icu/*:data_packaging=static

boost/*:without_atomic=True
boost/*:without_charconv=True
boost/*:without_chrono=True
boost/*:without_context=True
boost/*:without_contract=True
boost/*:without_coroutine=True
boost/*:without_date_time=True
boost/*:without_exception=True
boost/*:without_fiber=True
boost/*:without_filesystem=True
boost/*:without_graph=True
boost/*:without_json=True
boost/*:without_locale=True
boost/*:without_log=True
boost/*:without_math=True
boost/*:without_nowide=True
boost/*:without_process=True
boost/*:without_serialization=True
boost/*:without_stacktrace=True
boost/*:without_test=True
boost/*:without_thread=True
boost/*:without_timer=True
boost/*:without_type_erasure=True
boost/*:without_wave=True

# CMake-based dependencies pick up emcc via the Emscripten.cmake toolchain
# wired in below (`tools.cmake.cmaketoolchain:user_toolchain`). Autotools-
# based ones (icu, openssl) don't read CMake toolchains — they just consult
# CC/CXX/AR/... in the environment. The Conan Center emsdk recipe used to
# inject these for us; since we install emsdk directly now, we set them
# here. The unprefixed names rely on emcc/em++/emar/... being on PATH,
# which setup-emsdk (CI) and emsdk_env.sh (local) both arrange.
[buildenv]
CC=emcc
CXX=em++
AR=emar
NM=emnm
RANLIB=emranlib
STRIP=emstrip

[conf]
tools.cmake.cmaketoolchain:generator=Ninja
tools.cmake.cmaketoolchain:user_toolchain=['{{ os.environ.get("EMSDK", "") }}/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake']
tools.build:compiler_executables={"c": "emcc", "cpp": "em++"}
# Exception handling must use the same mode when compiling and linking, and
# across every translation unit and dependency: a mismatch makes wasm-opt fail
# validation. Hence `-fwasm-exceptions` appears in the compile flags, the link
# flags, the boost override, and the package_id below.
tools.build:cxxflags=['-fwasm-exceptions']
# Make the EH mode part of each dependency's package_id, so changing it rebuilds
# cached deps instead of silently linking them in a different EH mode.
tools.info.package_id:confs=['tools.build:cxxflags', 'tools.build:cflags']
# -m64 selects the wasm64 memory model (the non-deprecated spelling of the old
# -sMEMORY64=1). -sMAXIMUM_MEMORY=8GB uses wasm64's ability to exceed the 4GB
# wasm32 ceiling. -sSTACK_SIZE=8MB avoids startup stack overflows from QLever's
# deep recursion (ANTLR parser, query planner); the default stack is far smaller.
# -sUSE_ZLIB / -sUSE_BZIP2 supply the zlib/bzip2 headers the bundled spatialjoin
# dep #includes; they are ports because zlib/bzip2 are not conan dependencies.
tools.build:exelinkflags=['-sALLOW_MEMORY_GROWTH=1', '-sMAXIMUM_MEMORY=8GB', '-sINITIAL_MEMORY=64MB', '-m64', '-sUSE_ZLIB=1', '-sUSE_BZIP2=1', '-sSTACK_SIZE=8MB', '-fwasm-exceptions']
tools.build:sharedlinkflags=['-sALLOW_MEMORY_GROWTH=1', '-sMAXIMUM_MEMORY=8GB', '-sINITIAL_MEMORY=64MB', '-m64', '-sUSE_ZLIB=1', '-sUSE_BZIP2=1', '-sSTACK_SIZE=8MB', '-fwasm-exceptions']
# A package-specific list-conf replaces the global value rather than appending,
# so boost must restate -m64 (which the wasm64 arch otherwise supplies to
# CMake/autotools builds but not to boost's b2) alongside the EH mode.
boost/*:tools.build:cxxflags=['-m64', '-fwasm-exceptions']
