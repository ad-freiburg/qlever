// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

// Some builds of jemalloc (e.g. `jemalloc 5.4.0` from conda-forge) export
// `malloc`, `free`, `posix_memalign`, ... but NOT `aligned_alloc`. In such a
// process, every call to `aligned_alloc` (used e.g. by Boost.ASIO's
// `aligned_new` and by libstdc++'s over-aligned `operator new`) is served by
// glibc's allocator, while the matching `free` goes to jemalloc, which then
// crashes because it doesn't own the pointer. This file is only compiled and
// linked into the executables if CMake detects such a jemalloc (see the
// `JEMALLOC` section in the top-level `CMakeLists.txt`). It defines
// `aligned_alloc` in the executable, which takes precedence over the
// definition in glibc for the whole process, and forwards to jemalloc's
// `posix_memalign`.

#include <cerrno>
#include <cstddef>
#include <cstdlib>

extern "C" __attribute__((visibility("default"))) void* aligned_alloc(
    size_t alignment, size_t size) noexcept {
  // `posix_memalign` requires the alignment to be a power of two and a
  // multiple of `sizeof(void*)`.
  if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
    errno = EINVAL;
    return nullptr;
  }
  if (alignment < sizeof(void*)) {
    alignment = sizeof(void*);
  }
  void* result = nullptr;
  int error = posix_memalign(&result, alignment, size);
  if (error != 0) {
    errno = error;
    return nullptr;
  }
  return result;
}
