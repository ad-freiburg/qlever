//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "GlobalMemoryManagement.h"

#include <boost/stacktrace.hpp>

#include "../util/Log.h"

void* operator new(size_t sz) {
  if (sz == 0) {
    sz = 1;
  };
  if (sz == 0)
    ++sz;  // avoid std::malloc(0) which may return nullptr on success

  size_t limit = 1'000'000'000;
  if (sz > limit) {
    std::printf(
        "Encountered an unlimited allocation > %zu, printing stacktrace",
        limit);
    std::cout << boost::stacktrace::stacktrace() << std::endl;
  }

  if (void* ptr = std::malloc(sz)) return ptr;

  throw std::bad_alloc{};  // required by [new.delete.single]/3
}

void operator delete(void* ptr) noexcept { std::free(ptr); }
void operator delete(void* ptr, std::size_t n) noexcept { std::free(ptr); }
