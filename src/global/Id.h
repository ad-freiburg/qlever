// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_GLOBAL_ID_H
#define QLEVER_SRC_GLOBAL_ID_H

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "index/IdBitRepresentation.h"

// The implementation of `Id`. Every file that needs `Id` or `ValueId` includes
// this header, not the implementation header. The CMake variable
// `USE_SPLIT_LAYOUT` (not an `option()` yet, see `CMakeLists.txt`) chooses
// between the legacy `ValueId` and the `SplitLayoutId`.
//
// `BYTES_PER_ID_COLUMN_ENTRY` is the size of an `IdTable` column entry (memory
// and disk), smaller than the padded `sizeof(Id)` with the split layout.
#ifdef QLEVER_USE_SPLIT_LAYOUT
#include "engine/idTable/splitLayout/SplitLayoutId.h"
using Id = columnBasedIdTable::splitLayout::SplitLayoutId;
inline constexpr size_t BYTES_PER_ID_COLUMN_ENTRY =
    sizeof(Id::DatatypeType) + sizeof(Id::PayloadType);
#else
#include "global/FoldedValueId.h"
using Id = foldedValueId::ValueId;
inline constexpr size_t BYTES_PER_ID_COLUMN_ENTRY = sizeof(Id);
#endif
using ValueId = Id;
using Score = float;

// `IdBitRepresentation` stands in for `Id::T` in headers that cannot include
// this header because of an include cycle, so the two must be the same type.
static_assert(std::is_same_v<IdBitRepresentation, Id::T>);

// `Id`s are copied around a lot, in particular in bulk (see the `IdTable`
// class, in particular `IdTable::insertAtEnd`). Only for trivially copyable
// types do the compilers turn such a bulk copy into a single `std::memmove`
// instead of a scalar loop. Make sure that this property is not accidentally
// lost by adding a user-provided copy constructor, copy assignment operator,
// or destructor to `ValueId`.
static_assert(std::is_trivially_copyable_v<Id>);

// TODO<joka921> Make the following ID and index types strong.
using ColumnIndex = uint64_t;

// TODO<joka921> The following IDs only appear within the text index in the
// `Index` class, so they should not be public.
using WordIndex = uint64_t;
using WordOrEntityIndex = uint64_t;
using TextBlockIndex = uint64_t;
using CompressionCode = uint64_t;

#endif  // QLEVER_SRC_GLOBAL_ID_H
