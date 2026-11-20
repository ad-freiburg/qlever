// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_COLUMNSTORAGETRAITS_H
#define QLEVER_SRC_ENGINE_IDTABLE_COLUMNSTORAGETRAITS_H

#include "backports/span.h"
#include "engine/idTable/splitLayout/IdColumnVector.h"
#include "global/Id.h"

namespace columnBasedIdTable {

// The types that an `IdTable` with columns of type `ColumnStorage` hands out:
// a reference to a single element and a view of a whole column (both mutable
// and const). The default is for a `std::vector<T>`.
template <typename ColumnStorage, typename T>
struct ColumnStorageTraits {
  using Ref = T&;
  using ConstRef = const T&;
  using Column = ql::span<T>;
  using ConstColumn = ql::span<const T>;
};

// The split columns: a proxy instead of `Id&`, and a view of the two arrays
// instead of a span.
template <typename Allocator>
struct ColumnStorageTraits<splitLayout::IdColumnVector<Allocator>, Id> {
  using Storage = splitLayout::IdColumnVector<Allocator>;
  using Ref = typename Storage::reference_restricted;
  using ConstRef = typename Storage::const_reference_restricted;
  using Column = splitLayout::IdColumnRef;
  using ConstColumn = splitLayout::ConstIdColumnRef;
};

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COLUMNSTORAGETRAITS_H
