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
// this header, not the implementation header. The CMake option
// `USE_SPLIT_LAYOUT` chooses between the legacy `ValueId` and the
// `SplitLayoutId`.
#ifdef QLEVER_USE_ID_SPLIT_LAYOUT
#include "engine/idTable/splitLayout/SplitLayoutId.h"
using Id = columnBasedIdTable::splitLayout::SplitLayoutId;
#else
#include "global/FoldedValueId.h"
using Id = foldedValueId::ValueId;
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

// The number of bytes of one entry of an `IdTable` column, both in memory and
// in the packed byte representation on disk (index and spill files). Use it for
// the size of column entries, and `sizeof(Id)` only for the size of standalone
// `Id` objects.
//
// NOTE: The constant depends on the layout of `Id`, so it lives next to the
// definition of `Id` and has to change together with it. With a split layout
// (separate payload and datatype columns), a column entry is smaller than a
// standalone `Id`, which is padded.
inline constexpr size_t BYTES_PER_ID_COLUMN_ENTRY = sizeof(Id);

// TODO<joka921> Make the following ID and index types strong.
using ColumnIndex = uint64_t;

// TODO<joka921> The following IDs only appear within the text index in the
// `Index` class, so they should not be public.
using WordIndex = uint64_t;
using WordOrEntityIndex = uint64_t;
using TextBlockIndex = uint64_t;
using CompressionCode = uint64_t;

#endif  // QLEVER_SRC_GLOBAL_ID_H
