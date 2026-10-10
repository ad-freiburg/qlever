// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_BOOSTSORTHEADERS_H
#define QLEVER_SRC_UTIL_BLOCKSORT_BOOSTSORTHEADERS_H

// All the headers of Boost.Sort that QLever uses. Include this header instead
// of the Boost headers directly, because of two landmines:
// 1. `boost/sort/common/range.hpp` includes `<ciso646>`, which `#warning`s
//    about its own deprecation since C++20 (once per translation unit).
// 2. `boost/sort/common/util/circular_buffer.hpp` has a member named
//    `BLOCK_SIZE`, which `<linux/fs.h>` (included by `liburing.h`) defines as a
//    macro. The macro is not needed by QLever, so it is temporarily undefined
//    around the Boost headers (and restored afterwards).

#include "util/CompilerWarnings.h"

#pragma push_macro("BLOCK_SIZE")
#undef BLOCK_SIZE

DISABLE_PREPROCESSOR_WARNINGS
#include <boost/sort/block_indirect_sort/blk_detail/block.hpp>
#include <boost/sort/common/range.hpp>
#include <boost/sort/pdqsort/pdqsort.hpp>
REENABLE_PREPROCESSOR_WARNINGS

#pragma pop_macro("BLOCK_SIZE")

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_BOOSTSORTHEADERS_H
