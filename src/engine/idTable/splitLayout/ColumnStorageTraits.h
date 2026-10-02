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
#include "engine/idTable/splitLayout/IdColumn.h"
#include "engine/idTable/splitLayout/IdColumnVector.h"
#include "engine/idTable/splitLayout/IdRef.h"

namespace columnBasedIdTable::splitLayout {
// A customization point `IdTable` (see `IdTable.h`) uses to determine the
// reference/view types for one element resp. a whole column of its
// `ColumnStorage`. The primary template is a plain `std::vector<T, ...>`,
// used for any column type other than `Id` (e.g. `T = int` in tests).
template <typename ColumnStorage, typename T>
struct ColumnStorageTraits {
  using Ref = T&;
  using ConstRef = const T&;
  using ColumnRef = ql::span<T>;
  using ConstColumnRef = ql::span<const T>;
};

// Specialization for `Id` columns stored as an `IdColumnVector` (a structure
// of arrays, see there).
template <typename Allocator>
struct ColumnStorageTraits<IdColumnVector<Allocator>, Id> {
  using Ref = IdRef;
  using ConstRef = ConstIdRef;
  using ColumnRef = IdColumnRef;
  using ConstColumnRef = ConstIdColumnRef;
};
}  // namespace columnBasedIdTable::splitLayout

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COLUMNSTORAGETRAITS_H
