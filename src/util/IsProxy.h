// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ISPROXY_H
#define QLEVER_SRC_UTIL_ISPROXY_H

#include <type_traits>

namespace qlever {
// Specialize `isProxy` for types that are objects, but behave like references
// (e.g. the `IdRef` of the split layout, or the row references of the
// column-major `IdTable`). The `isProxy` variable is for example used by
// `ad_utility::tie` (see `util/Tie.h`).
template <typename T>
inline constexpr bool isProxy = false;

// The proxy type in which a proxy `T` that is returned by value is stored, for
// example by `ad_utility::tie`. By default `T` itself. A restricted proxy (one
// that can only be written to as an rvalue, like the proxy returned by
// `column[i]` of the split layout) declares the full proxy that can be written
// to like a reference as its member type `FullProxyType` (see
// `splitLayout/IdRef.h`).
template <typename T, typename = void>
struct FullProxy {
  using type = T;
};

template <typename T>
struct FullProxy<T, std::void_t<typename T::FullProxyType>> {
  using type = typename T::FullProxyType;
};

template <typename T>
using FullProxyT = typename FullProxy<T>::type;
}  // namespace qlever

#endif  // QLEVER_SRC_UTIL_ISPROXY_H
