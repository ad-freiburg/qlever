// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#ifndef QLEVER_JTHREAD_H

#include <thread>
#include <version>

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
