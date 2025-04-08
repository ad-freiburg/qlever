// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_GLOBAL_ID_H
#define QLEVER_SRC_GLOBAL_ID_H

#include <cstdint>

#include "global/ValueId.h"

using Id = ValueId;
using Score = float;

// TODO<joka921> Make the following ID and index types strong.
using ColumnIndex = uint64_t;

// TODO<joka921> The following IDs only appear within the text index in the
// `Index` class, so they should not be public.
using WordIndex = uint64_t;
using WordOrEntityIndex = uint64_t;
using TextBlockIndex = uint64_t;
using CompressionCode = uint64_t;

#endif  // QLEVER_SRC_GLOBAL_ID_H
