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
//
// 1. `boost/sort/common/range.hpp` includes the `<ciso646>` of libstdc++,
//    which since C++20 is deprecated and warns about itself (from libstdc++ 15
//    on). Treating the `boost` headers as system headers doesn't help: the
//    warning is a `#warning` directive, which both GCC and Clang report even
//    inside system headers. Only disabling `-Wcpp` (GCC) or `-W#warnings`
//    (Clang) silences it, see `util/CompilerWarnings.h`. The warning has to be
//    disabled exactly once, at the place where the headers are first seen.
//    Disabling it at the consumers instead would not work reliably: the
//    include guards make it a matter of chance which of them includes the
//    headers first. `BlockIndirectSort.h` is included by
//    `engine/idTable/CompressedExternalIdTable.h` and hence by almost every
//    translation unit of QLever, so that warning would otherwise appear once
//    per translation unit, and QLever's CI compiles with `-Werror`.
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
