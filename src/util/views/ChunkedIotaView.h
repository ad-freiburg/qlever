// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_VIEWS_CHUNKEDIOTAVIEW_H
#define QLEVER_SRC_UTIL_VIEWS_CHUNKEDIOTAVIEW_H

#include <range/v3/view/chunk.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/transform.hpp>
#include <type_traits>
#include <utility>

#include "backports/concepts.h"
#include "util/Exception.h"

namespace ad_utility {
// Split the half-open integer range `[first, end)` into consecutive chunks of
// `blockSize` elements each (the last chunk may be smaller) and return a view
// that yields each chunk as a `std::pair{chunkBegin, chunkEnd}` of half-open
// bounds. For example, `chunkedIotaView(0, 7, 3)` yields `{0, 3}`, `{3, 6}`,
// `{6, 7}`.
//
// NOTE: In contrast to `ql::views::iota`, the `first` and the `end` must have
// exactly the same type. `iota` silently accepts mixed types (e.g. `int` and
// `size_t`), which can lead to surprising results or to infinite ranges.
CPP_template(typename T)(requires std::is_integral_v<T>) auto chunkedIotaView(
    T first, T end, T blockSize) {
  AD_CONTRACT_CHECK(first <= end);
  AD_CONTRACT_CHECK(blockSize > 0);
  namespace rv = ::ranges::views;
  return rv::iota(first, end) | rv::chunk(blockSize) |
         rv::transform([](const auto& chunk) {
           T chunkBegin = *chunk.begin();
           return std::pair{chunkBegin,
                            static_cast<T>(chunkBegin + ::ranges::size(chunk))};
         });
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VIEWS_CHUNKEDIOTAVIEW_H
