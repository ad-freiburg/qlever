// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_GLOBAL_ID_H
#define QLEVER_SRC_GLOBAL_ID_H

#include <cstdint>
#include <type_traits>

#include "global/ValueId.h"

using Id = ValueId;
using Score = float;

// `Id`s are copied around a lot, in particular in bulk (see the `IdTable`
// class, in particular `IdTable::insertAtEnd`). Only for trivially copyable
// types do the compilers turn such a bulk copy into a single `std::memmove`
// instead of a scalar loop. Make sure that this property is not accidentally
// lost by adding a user-provided copy constructor, copy assignment operator,
// or destructor to `ValueId`.
static_assert(std::is_trivially_copyable_v<Id>);

// Lambda, not `&Id::<function>`: proxy column elements
// don't support pointer-to-member dispatch (see `IdColumn.h`). The
// lambda's `const Id&` parameters trigger the proxy's implicit
// conversion to `Id` instead.
inline bool isUndefinedId(const Id& id) { return id.isUndefined(); }

inline auto compareIdsWithoutLocalVocab(const Id& a, const Id& b) {
  return a.compareWithoutLocalVocab(b);
}

inline auto getIdBits(const Id& id) { return id.getBits(); }

// TODO<joka921> Make the following ID and index types strong.
using ColumnIndex = uint64_t;

// TODO<joka921> The following IDs only appear within the text index in the
// `Index` class, so they should not be public.
using WordIndex = uint64_t;
using WordOrEntityIndex = uint64_t;
using TextBlockIndex = uint64_t;
using CompressionCode = uint64_t;

#endif  // QLEVER_SRC_GLOBAL_ID_H
