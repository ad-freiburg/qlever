// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_RANGETHATRELEASESONEND_H
#define QLEVER_SRC_UTIL_RANGETHATRELEASESONEND_H

#include <optional>
#include <utility>

#include "util/Iterators.h"

namespace ad_utility {
// A lazy input range that owns the range it yields from and drops that range
// (and hence everything that range owns) as soon as it is exhausted, instead of
// only when this object is destroyed. An exception that the inner range throws
// releases it likewise. Afterwards this range simply has nothing left to yield.
//
// This matters whenever the consumer of a range legitimately keeps the
// (exhausted) range alive, but the resources that the inner range holds have to
// be freed as soon as all of its elements have been consumed.
template <typename T>
class RangeThatReleasesOnEnd : public InputRangeFromGet<T> {
 private:
  std::optional<InputRangeTypeErased<T>> range_;

 public:
  // Construct from the `range` to yield from and to release on its end.
  explicit RangeThatReleasesOnEnd(InputRangeTypeErased<T> range)
      : range_{std::move(range)} {}

  // Yield the next element of the inner range, and release that range as soon
  // as it is over or has thrown.
  std::optional<T> get() override {
    if (!range_.has_value()) {
      return std::nullopt;
    }
    std::optional<T> element;
    try {
      element = range_.value().get();
    } catch (...) {
      // The consumer of a range that has thrown may well keep that range alive
      // for a long time, so release the inner range here as well.
      range_.reset();
      throw;
    }
    if (!element.has_value()) {
      range_.reset();
    }
    return element;
  }
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_RANGETHATRELEASESONEND_H
