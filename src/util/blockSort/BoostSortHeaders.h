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

// The headers of `boost::sort` used by the block indirect sort, with the
// deprecation warning of `<ciso646>` (included by Boost) disabled.
DISABLE_PREPROCESSOR_WARNINGS
#include <boost/sort/block_indirect_sort/blk_detail/block.hpp>
#include <boost/sort/common/pivot.hpp>
#include <boost/sort/common/range.hpp>
#include <boost/sort/common/util/algorithm.hpp>
#include <boost/sort/common/util/merge.hpp>
#include <boost/sort/pdqsort/pdqsort.hpp>
REENABLE_PREPROCESSOR_WARNINGS

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_BOOSTSORTHEADERS_H
