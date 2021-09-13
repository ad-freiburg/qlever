//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_ONDESTRUCTION_H
#define QLEVER_ONDESTRUCTION_H

namespace ad_utility {

/// A simple type that executes a specified action at the time it is destroyed
/// F must be callable without arguments, return void and be noexcept.
template <typename F>
requires std::is_nothrow_invocable_r_v<void, F> class OnDestruction {
 private:
  F f_;

 public:
  OnDestruction(F f) : f_{std::move(f)} {}
  ~OnDestruction() { f_(); }
};
}  // namespace ad_utility

#endif  // QLEVER_ONDESTRUCTION_H
