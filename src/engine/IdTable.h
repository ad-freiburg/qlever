// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#pragma once

#include "engine/IdTableColumnBased.h"
#include "util/AllocatorWithLimit.h"
#include "util/UninitializedAllocator.h"

namespace detail {
using defaultAllocator =
    ad_utility::default_init_allocator<Id, ad_utility::AllocatorWithLimit<Id>>;
}

/// The general IdTable class. Can be modified and owns its data. If COLS > 0,
/// COLS specifies the compile-time number of columns COLS == 0 means "runtime
/// number of numColumns"
template <int COLS, typename Allocator = detail::defaultAllocator>
using IdTableStatic = columnBasedIdTable::IdTable<COLS, Allocator>;

// This was previously implemented as an alias (`using IdTable =
// IdTableStatic<0, ...>`). However this did not allow forward declarations, so
// we now implement `IdTable` as a subclass of `IdTableStatic<0, ...>` that can
// be implicitly converted to and from `IdTableStatic<0, ...>`.
class IdTable : public IdTableStatic<0, detail::defaultAllocator> {
 public:
  using Base = IdTableStatic<0, detail::defaultAllocator>;
  // Inherit the constructors.
  using Base::Base;

  IdTable(Base&& b) : Base(std::move(b)) {}
  IdTable(const Base& b) : Base(b) {}

  IdTable& operator=(const Base& b) {
    *(static_cast<Base*>(this)) = b;
    return *this;
  }

  IdTable& operator=(Base&& b) {
    *(static_cast<Base*>(this)) = std::move(b);
    return *this;
  }
};

/// A constant view into an IdTable that does not own its data
template <int COLS, typename Allocator = detail::defaultAllocator>
using IdTableView =
    columnBasedIdTable::IdTable<COLS, Allocator,
                                columnBasedIdTable::IsView::True>;
