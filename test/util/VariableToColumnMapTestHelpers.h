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

#include <utility>
#include <vector>

#include "./AllocatorTestHelpers.h"
#include "engine/VariableToColumnMap.h"

// Return a `VariableToColumnMap` with the given `entries` that uses the
// unlimited allocator of `ad_utility::testing::makeAllocator()`. A functor, so
// that tests can bind it to a shorter name.
struct MakeVariableToColumnMap {
  VariableToColumnMap operator()(
      std::vector<std::pair<Variable, ColumnIndexAndTypeInfo>> entries = {})
      const {
    VariableToColumnMap map{ad_utility::testing::makeAllocator()};
    for (auto& [variable, column] : entries) {
      map.try_emplace(std::move(variable), column);
    }
    return map;
  }
};
inline constexpr MakeVariableToColumnMap makeVariableToColumnMap{};

#endif  // QLEVER_TEST_UTIL_VARIABLETOCOLUMNMAPTESTHELPERS_H
