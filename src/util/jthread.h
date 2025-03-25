// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#ifndef QLEVER_JTHREAD_H

#include <thread>
#include <version>

// While libc++ has no std::jthread (threads that automatically join on
// destruction), we ship our own.
#ifdef _LIBCPP_VERSION
namespace ad_utility {
struct JThread : public std::thread {
#ifdef __cpp_lib_jthread
  static_assert(false,
                "std::jthread is now supported by libc++, get rid of "
                "ad_utility::JThread");
#endif
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
// libstdc++ already supports jthread, simply use it.
#ifndef __cpp_lib_jthread
static_assert(false,
              "std::jthread is not supported by the version of libstdc++ you "
              "are using, please update or use QLever inside of Docker");
#endif
namespace ad_utility {
using JThread = std::jthread;
}

#endif

#define QLEVER_JTHREAD_H

#endif  // QLEVER_JTHREAD_H
