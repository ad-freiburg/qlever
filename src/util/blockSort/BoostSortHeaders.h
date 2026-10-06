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

#include "util/CompilerWarnings.h"

// The headers of `boost::sort` that the block indirect sort (see
// `util/blockSort/BlockIndirectSort.h`) is built on and that include the
// `<ciso646>` of libstdc++, which since C++20 is deprecated and warns about
// itself (from libstdc++ 15 on).
//
// Treating the `boost` headers as system headers doesn't help: the warning is
// a `#warning` directive, which both GCC and Clang report even inside system
// headers. Only disabling `-Wcpp` (GCC) or `-W#warnings` (Clang) silences it.
//
// They are included here instead of directly, so that the warning is disabled
// exactly once, at the place where the headers are first seen. Disabling it at
// the consumers instead would not work reliably: the include guards make it a
// matter of chance which of them includes the headers first.
//
// `BlockIndirectSort.h` is included by
// `engine/idTable/CompressedExternalIdTable.h` and hence by almost every
// translation unit of QLever, so that warning would otherwise appear once per
// translation unit, and QLever's CI compiles with `-Werror`.
DISABLE_PREPROCESSOR_WARNINGS
#include <boost/sort/block_indirect_sort/blk_detail/block.hpp>
#include <boost/sort/common/range.hpp>
REENABLE_PREPROCESSOR_WARNINGS

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_BOOSTSORTHEADERS_H
