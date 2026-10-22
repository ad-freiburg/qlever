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

namespace qlever {
// Specialize `isProxy` for types that are objects, but behave like references
// (e.g. the `IdRef` of the split layout, or the row references of the
// column-major `IdTable`). The `isProxy` variable is for example used by
// `ad_utility::tie` (see `util/Tie.h`).
template <typename T>
inline constexpr bool isProxy = false;
}  // namespace qlever

#endif  // QLEVER_SRC_UTIL_ISPROXY_H
