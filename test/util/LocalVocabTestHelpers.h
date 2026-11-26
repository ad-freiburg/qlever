// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_LOCALVOCABTESTHELPERS_H
#define QLEVER_TEST_UTIL_LOCALVOCABTESTHELPERS_H

#include "./AllocatorTestHelpers.h"
#include "index/LocalVocab.h"

namespace ad_utility::testing {

// Return an empty `LocalVocab` with an allocator without a memory limit, for
// tests in which the allocator of the `LocalVocab` doesn't matter.
inline LocalVocab makeLocalVocab() { return LocalVocab{makeAllocator()}; }

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_LOCALVOCABTESTHELPERS_H
