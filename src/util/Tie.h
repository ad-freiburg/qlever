// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_TIE_H
#define QLEVER_SRC_UTIL_TIE_H

#include <tuple>

#include "backports/type_traits.h"
#include "util/IsProxy.h"

namespace ad_utility {

// Return the type in which `tie` stores an argument of type `T`, as a
// `ql::type_identity`. An lvalue stays a reference, a proxy that is returned by
// value is stored by value.
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

// Like `std::tie`, but it also accepts proxies that are returned by value (like
// `IdRef`), which `std::tie` cannot bind. Lvalues are stored as references,
// proxies by value, other temporaries are rejected (see `TieElement`).
template <typename... Ts>
constexpr auto tie(Ts&&... args) {
  return std::tuple<TieElement<Ts>...>(std::forward<Ts>(args)...);
}

// Like `tie(row[I]...)`, but the indices come from a compile-time
// sequence (`std::index_sequence` or `ad_utility::ValueSequence`) instead of
// being written out. Lvalues are tied by reference,
// proxies that are returned by value are stored by value.
template <typename Row, template <typename T, T...> typename Tp, size_t... I>
constexpr auto tieElementsByIndexSequence(Row& row, Tp<size_t, I...>) {
  return tie(row[I]...);
}

}  // namespace ad_utility
#endif  // QLEVER_SRC_UTIL_TIE_H
