//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_ONDESTRUCTION_H
#define QLEVER_ONDESTRUCTION_H

namespace ad_utility {

/// A simple type that executes a specified action at the time it is destroyed
/// F must be callable without arguments return void. If F throws an exception,
/// then the destructor of `OnDestruction` is `noexcept(false)`. This is the
/// major difference to `absl::Cleanup`, the destructor of which is always
/// noexcept, which will lead to the program being aborted when F throws.
template <typename F>
requires std::is_invocable_r_v<void, F> class OnDestruction {
 private:
  F f_;

 public:
  explicit OnDestruction(F f) : f_{std::move(f)} {}
  ~OnDestruction() noexcept(noexcept(f_())) { f_(); }
};
}  // namespace ad_utility

#endif  // QLEVER_ONDESTRUCTION_H
