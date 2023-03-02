//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_ONDESTRUCTION_H
#define QLEVER_ONDESTRUCTION_H

#include <concepts>
#include <exception>
#include <iostream>

#include "util/SourceLocation.h"

namespace ad_utility {

/// A simple type that executes a specified action at the time it is destroyed
/// F must be callable without arguments return void. If F throws an exception,
/// then the destructor of `OnDestruction` is `noexcept(false)`. This is the
/// major difference to `absl::Cleanup`, the destructor of which is always
/// noexcept, which will lead to the program being aborted when F throws.
template <typename F>
requires std::is_invocable_r_v<void, F>
class OnDestruction {
 private:
  F f_;

 public:
  explicit OnDestruction(F f) : f_{std::move(f)} {}
  ~OnDestruction() noexcept(noexcept(f_())) { f_(); }
};

// Call `f()`. If this call throws, then terminate the program after logging an
// error message that includes the `message`, information on the thrown
// exception, and the location of the call. This can be used to make destructors
// `noexcept()` that have to perform some non-trivial logic (e.g. writing a
// trailer to a file), when such a failure should never occur in practice and
// also is not easily recovarable. For an example usage see `PatternCreator.h`.
// The actual termination call can be configured for testing purposes. Note that
// this function must never throw an exception.
template <std::invocable<> F>
void terminateIfThrows(F&& f, std::string_view message,
                       void (*terminateAction)() noexcept = &std::terminate,
                       ad_utility::source_location l =
                           ad_utility::source_location::current()) noexcept {
  try {
    std::invoke(f);
  } catch (const std::exception& e) {
    std::cerr << "A function that should never throw has thrown an exception "
                 "with message \""
              << e.what() << "\". The function was called in file "
              << l.file_name() << " on line " << l.line()
              << ". Additional information: " << message
              << ". Please report this. Terminating" << std::endl;
    terminateAction();
  } catch (...) {
    std::cerr << "A function that should never throw has thrown an exception. "
                 "The function was called in file "
              << l.file_name() << " on line " << l.line()
              << ". Additional information: " << message
              << ". Please report this. Terminating" << std::endl;
    terminateAction();
  }
}
}  // namespace ad_utility

#endif  // QLEVER_ONDESTRUCTION_H
