//
// Created by projector-user on 10/6/21.
//

#ifndef QLEVER_JTHREAD_H

#include <version>
#include <thread>

// While libc++ has no std::jthread, we ship our own
#ifdef _LIBCPP_VERSION
namespace ad_utility {
struct JThread : public std::thread {
#ifdef _cpp_lib_jthread
  static_assert(false, "std::jthread is now supported by libc++, get rid of ad_utility::Jthread");
#endif
  using Base = std::thread;
  using Base::Base;
  JThread(JThread&&) noexcept = default;
  ~JThread() {
    if (joinable()) {
      join();
    }
  }
};
}

#else
// libstdc++ already supports jthread, simply use it
#ifndef _cpp_lib_jthread
  static_assert(false, "std::jthread is not supported by the version of libstdc++ you are using, please update or using QLever inside of Docker");
#endif
namespace ad_utility {
using JThread = std::jthread;
}

#endif

#define QLEVER_JTHREAD_H

#endif  // QLEVER_JTHREAD_H
