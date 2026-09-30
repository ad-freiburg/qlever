// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_COLUMNSTORAGETRAITS_H
#define QLEVER_SRC_ENGINE_IDTABLE_COLUMNSTORAGETRAITS_H

#include "backports/span.h"

namespace columnBasedIdTable {

// Determines `IdTable`'s reference/view types for a single element resp. a
// whole column of a given `ColumnStorage`.
template <typename ColumnStorage, typename T>
struct ColumnStorageTraits {
  using Ref = T&;
  using ConstRef = const T&;
  using Column = ql::span<T>;
  using ConstColumn = ql::span<const T>;
};

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COLUMNSTORAGETRAITS_H
