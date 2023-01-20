// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (Januar of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <ranges>
#include <sstream>
#include <tuple>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"
#include "util/Forward.h"
#include "util/Random.h"

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}

auto I = [](const auto& id) {
  return Id::makeFromVocabIndex(VocabIndex::make(id));
};

/*
 * Does what it says on the tin: Save an IdTable with the corresponding
 * join column.
 */
struct IdTableAndJoinColumn {
  IdTable idTable;
  size_t joinColumn;
};

// For easier reading. We repeat that type combination so often, that this
// will make things a lot easier in terms of reading and writing.
using VectorTable = std::vector<std::vector<size_t>>;

/*
 * Return an 'IdTable' with the given 'tableContent'. all rows must have the
 * same length.
 */
IdTable makeIdTableFromVector(const VectorTable& tableContent) {
  AD_CHECK(!tableContent.empty());
  IdTable result{tableContent[0].size(), allocator()};

  // Copying the content into the table.
  for (const auto& row : tableContent) {
    AD_CHECK(row.size() == result.numColumns());  // All rows of an IdTable must
                                                  // have the same length.
    const size_t backIndex{result.size()};

    // TODO<clang 16> This should be
    // std::ranges::copy(std::views::transform(row, I), result.back().begin());
    // as soon as our compilers supports it.
    result.emplace_back();

    for (size_t c = 0; c < row.size(); c++) {
      result(backIndex, c) = I(row[c]);
    }
  }

  return result;
}
