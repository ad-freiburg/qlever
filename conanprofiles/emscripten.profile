[settings]
arch=wasm64
build_type=Release
compiler=emcc
compiler.cppstd=20
compiler.libcxx=libc++
# emsdk 3.1.73 is the latest version provided by the Conan package manager.
compiler.version=3.1.73
os=Emscripten
compiler.threads=posix

[tool_requires]
emsdk/3.1.73

[options]
icu/*:with_icuio=False

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

[conf]
tools.cmake.cmaketoolchain:generator=Ninja
tools.build:exelinkflags=['-sALLOW_MEMORY_GROWTH=1', '-sMAXIMUM_MEMORY=4GB', '-sINITIAL_MEMORY=64MB', '-sMEMORY64=1', '-sUSE_ICU=1', '-sUSE_BOOST_HEADERS=1', '-sUSE_ZLIB=1', '-sUSE_BZIP2=1', '-fexceptions']
tools.build:sharedlinkflags=['-sALLOW_MEMORY_GROWTH=1', '-sMAXIMUM_MEMORY=4GB', '-sINITIAL_MEMORY=64MB', '-sMEMORY64=1', '-sUSE_ICU=1', '-sUSE_BOOST_HEADERS=1', '-sUSE_ZLIB=1', '-sUSE_BZIP2=1', '-fexceptions']
boost/*:tools.build:cxxflags=['-sMEMORY64=1']
