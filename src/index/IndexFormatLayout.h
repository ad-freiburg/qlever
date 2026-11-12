// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_INDEXFORMATLAYOUT_H
#define QLEVER_SRC_INDEX_INDEXFORMATLAYOUT_H

#include "util/OnDiskLayout.h"

// Assert the layout of a `Type` whose bytes are written to the files of the
// index as they are, such that a change of the layout is a change of the index
// format. See `QL_ASSERT_ON_DISK_LAYOUT` in `util/OnDiskLayout.h` for the
// details, in particular for the syntax of the `expectedLayout`.
//
// NOTE: This header deliberately doesn't include `index/IndexFormatVersion.h`,
// because it is included by very basic headers (like the one of the `ValueId`).
#define QL_ASSERT_INDEX_FORMAT_LAYOUT(Type, expectedLayout)                    \
  QL_ASSERT_ON_DISK_LAYOUT(                                                    \
      Type, expectedLayout,                                                    \
      "The bytes of this class are written to the files of the index as they " \
      "are, so this is a change of the index format. If the change is "        \
      "intended, update `indexFormatVersion` (follow the instructions in "     \
      "`index/IndexFormatVersion.h`), and then also update the layout that "   \
      "is asserted for this class")

#endif  // QLEVER_SRC_INDEX_INDEXFORMATLAYOUT_H
