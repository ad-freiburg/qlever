// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#ifndef QLEVER_JTHREAD_H

#include <thread>
// The `<version>` header (needed below only for the `__cpp_lib_jthread`
// feature-test macro) does not exist in GCC 8's libstdc++. This is not a
// problem under `QLEVER_CPP_17`, because `std::jthread` never exists there
// either, so `__cpp_lib_jthread` being simply undefined still selects the
// correct (`ad_utility`-provided) branch below.
#ifndef QLEVER_CPP_17
#include <version>
#endif

// For standard libraries that do not yet have `std::jthread`, we roll our own.
#ifndef __cpp_lib_jthread
namespace ad_utility {
struct JThread : public std::thread {
  using Base = std::thread;
  using Base::Base;
  JThread(JThread&&) noexcept = default;
  JThread& operator=(JThread&& other) noexcept {
    if (joinable()) {
      join();
    }
    Base::operator=(std::move(other));
    return *this;
  }
  ~JThread() {
    if (joinable()) {
      join();
    }
  }
};
}  // namespace ad_utility

#else
// Use `std::jthread` if supported.
namespace ad_utility {
using JThread = std::jthread;
}

#endif

#define QLEVER_JTHREAD_H

#endif  // QLEVER_JTHREAD_H
