//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <algorithm>
#include <functional>
#include <range/v3/all.hpp>
#include <utility>

// The following defines namespaces `ql::ranges` and `ql::views` that are almost
// drop-in replacements for `std::ranges` and `std::views`. In C++20 mode (when
// the `QLEVER_CPP_17` macro is not used), these namespaces are simply aliases
// for `std::ranges` and `std::views`. In C++17 mode they contain the ranges and
// views from Erice Niebler's `range-v3` library. NOTE: `ql::ranges::unique`
// currently doesn't work, because the interface to this function is different
// in both implementations. NOTE: There might be other caveats which we are
// currently not aware of, because they only affect functions that we currently
// don't use. For those, the following header can be expanded in the future.
#ifndef QLEVER_CPP_17
#include <concepts>
#include <ranges>
#endif

namespace ql {

namespace ranges {
#ifdef QLEVER_CPP_17
using namespace ::ranges;

// The `view` concept (which is rather important when implementing custom views)
// is in a different namespace in range-v3, so we make it manually accessible.
template <typename T>
CPP_concept view = ::ranges::cpp20::view<T>;
#else
using namespace std::ranges;
#endif
}  // namespace ranges

namespace views {
#ifdef QLEVER_CPP_17
using namespace ::ranges::views;
#else
using namespace std::views;
#endif
}  // namespace views

// The namespace `ql::concepts` includes concepts that are contained in the
// C++20 standard as well as in `range-v3`.
namespace concepts {
#ifdef QLEVER_CPP_17
using namespace ::concepts;
#else
using namespace std;
#endif
}  // namespace concepts

}  // namespace ql
