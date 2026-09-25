// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_COLUMNSTORAGETRAITS_H
#define QLEVER_SRC_ENGINE_IDTABLE_COLUMNSTORAGETRAITS_H

#include "backports/span.h"
#include "engine/idTable/IdColumn.h"
#include "engine/idTable/IdColumnVector.h"
#include "engine/idTable/IdRef.h"

namespace columnBasedIdTable {

// A customization point `IdTable` (see `IdTable.h`) uses to determine the
// reference/view types for one element resp. a whole column of its
// `ColumnStorage`. The primary template is a plain `std::vector<T, ...>`,
// used for any column type other than `Id` (e.g. `T = int` in tests).
template <typename ColumnStorage, typename T>
struct ColumnStorageTraits {
  using Ref = T&;
  using ConstRef = const T&;
  using Column = ql::span<T>;
  using ConstColumn = ql::span<const T>;
};

// Specialization for `Id` columns stored as an `IdColumnVector` (a structure
// of arrays, see there).
template <typename Allocator>
struct ColumnStorageTraits<IdColumnVector<Allocator>, Id> {
  using Ref = IdRef;
  using ConstRef = ConstIdRef;
  using Column = IdColumn;
  using ConstColumn = ConstIdColumn;
};

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COLUMNSTORAGETRAITS_H
