// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_VARIABLETOCOLUMNMAPTESTHELPERS_H
#define QLEVER_TEST_UTIL_VARIABLETOCOLUMNMAPTESTHELPERS_H

#include <initializer_list>

#include "./AllocatorTestHelpers.h"
#include "engine/VariableToColumnMap.h"

// Return a `VariableToColumnMap` with the given `entries` that uses the
// unlimited allocator of `ad_utility::testing::makeAllocator()`.
inline VariableToColumnMap makeVariableToColumnMap(
    std::initializer_list<VariableToColumnMap::value_type> entries = {}) {
  return VariableToColumnMap{entries, ad_utility::testing::makeAllocator()};
}

#endif  // QLEVER_TEST_UTIL_VARIABLETOCOLUMNMAPTESTHELPERS_H
