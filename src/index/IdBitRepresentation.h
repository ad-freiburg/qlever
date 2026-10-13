// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_IDBITREPRESENTATION_H
#define QLEVER_SRC_INDEX_IDBITREPRESENTATION_H

// To prevent cycle includes in LocalVocabEntry.h,
// we need this small extra header in addition to Id.h
#ifdef QLEVER_USE_ID_SPLIT_LAYOUT
#include "engine/idTable/splitLayout/SplitLayoutIdBitRepresentation.h"
using IdBitRepresentation =
    columnBasedIdTable::splitLayout::SplitLayoutIdBitRepresentation;
#else
#include <cstdint>
using IdBitRepresentation = uint64_t;
#endif

#endif  // QLEVER_SRC_INDEX_IDBITREPRESENTATION_H
