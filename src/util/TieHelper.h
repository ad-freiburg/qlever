// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_TIEHELPER_H
#define QLEVER_SRC_UTIL_TIEHELPER_H

#include "backports/type_traits.h"

namespace qlever {
// Specialize to `true` for types that are cheap to copy and act as a reference
// to an element (like the proxies of split layout).
template <typename T>
inline constexpr bool isProxy = false;
}  // namespace qlever

namespace ad_utility {
// lvalues stay references, a proxy that is returned by value is stored by value
template <typename T>
constexpr auto tieElement() {
  if constexpr (std::is_lvalue_reference_v<T>) {
    return ql::type_identity<T>{};
  } else {
    static_assert(qlever::isProxy<std::decay_t<T> >,
                  "A tied element that is returned by value must be a proxy. "
                  "Mark such a type via `qlever::isProxy`.");
    return ql::type_identity<std::decay_t<T> >{};
  }
}

template <typename T>
using TieElement = typename decltype(tieElement<T>())::type;

// This code works for `std::integer_sequence` as well as
// `ad_utility::ValueSequence`.
// TODO<pas-kes>: Test/benchmark this with proxies of the split layout in future
template <typename Row, template <typename T, T...> typename Tp, size_t... I>
auto tieHelper(Row& row, Tp<size_t, I...>) {
  return std::tuple<TieElement<decltype(row[I])>...>(row[I]...);
}
}  // namespace ad_utility
#endif  // QLEVER_SRC_UTIL_TIEHELPER_H
