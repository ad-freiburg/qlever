// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_IDBITREPRESENTATION_H
#define QLEVER_SRC_INDEX_IDBITREPRESENTATION_H

// The type of the bits of an `Id` (`Id::T`), for headers that cannot include
// `global/Id.h` because of an include cycle, like `LocalVocabEntry.h`.
// `global/Id.h` checks that the two types are the same.
#ifdef QLEVER_USE_ID_SPLIT_LAYOUT
#include "engine/idTable/splitLayout/SplitLayoutIdBitRepresentation.h"
using IdBitRepresentation =
    columnBasedIdTable::splitLayout::SplitLayoutIdBitRepresentation;
#else
#include <cstdint>
using IdBitRepresentation = uint64_t;
#endif

#endif  // QLEVER_SRC_INDEX_IDBITREPRESENTATION_H
