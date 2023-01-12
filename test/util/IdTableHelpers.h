// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (Januar of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <tuple>

#include "./GTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"
#include "util/Forward.h"
#include "util/Random.h"
#include "util/SourceLocation.h"

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}

auto I = [](const auto& id) {
  return Id::makeFromVocabIndex(VocabIndex::make(id));
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
    // TODO This should be
    // std::ranges::copy(std::views::transform(row, I), result.back().begin());
    // as soon as our compilers and the IdTable support it.
    result.emplace_back();

    for (size_t c = 0; c < row.size(); c++) {
      result(backIndex, c) = I(row[c]);
    }
  }

  return result;
}

/*
 * @brief Tests, whether the given IdTable has the same content as the sample
 * solution and, if the option was choosen, if the IdTable is sorted by
 * the join column.
 *
 * @param table The IdTable that should be tested.
 * @param expectedContent The sample solution. Doesn't need to be sorted,
 *  or the same order of rows as the table.
 * @param resultMustBeSortedByJoinColumn If this is true, it will also be
 * tested, if the table is sorted by the join column.
 * @param joinColumn The join column of the table.
 * @param l Ignore it. It's only here for being able to make better messages,
 *  if a IdTable fails the comparison.
 */
void compareIdTableWithExpectedContent(
    const IdTable& table, const IdTable& expectedContent,
    const bool resultMustBeSortedByJoinColumn = false,
    const size_t joinColumn = 0,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating more informative messages, when failing the comparison.
  std::stringstream traceMessage{};

  auto writeIdTableToStream = [&traceMessage](const IdTable& idTable) {
    std::ranges::for_each(idTable,
                          [&traceMessage](const auto& row) {
                            // TODO Could be done in one line, if row was
                            // iterable. Unfortunaly, it isn't.
                            for (size_t i = 0; i < row.numColumns(); i++) {
                              traceMessage << row[i] << " ";
                            }
                            traceMessage << "\n";
                          },
                          {});
  };

  traceMessage << "compareIdTableWithExpectedContent comparing IdTable\n";
  writeIdTableToStream(table);
  traceMessage << "with IdTable \n";
  writeIdTableToStream(expectedContent);
  auto trace{generateLocationTrace(l, traceMessage.str())};

  // Because we compare tables later by sorting them, so that every table has
  // one definit form, we need to create local copies.
  IdTable localTable{table.clone()};
  IdTable localExpectedContent{expectedContent.clone()};

  if (resultMustBeSortedByJoinColumn) {
    // Is the table sorted by join column?
    ASSERT_TRUE(std::ranges::is_sorted(localTable.getColumn(joinColumn)));
  }

  // Sort both the table and the expectedContent, so that both have a definite
  // form for comparison.
  // TODO Instead of this, we could use std::ranges::lexicographical_compare
  // for the body of the lambda, but that is currently not possible, because
  // the rows of an IdTable are not iterable.
  auto sortFunction = [](const auto& row1, const auto& row2) {
    size_t i{0};
    while (i < (row1.numColumns() - 1) && row1[i] == row2[i]) {
      i++;
    }
    return row1[i] < row2[i];
  };
  /*
   * TODO Introduce functionality to the IdTable-class, so that
   * std::ranges::sort can be used instead of std::sort. Currently it seems like
   * the iterators , produced by IdTable, aren't the right type.
   */
  std::sort(localTable.begin(), localTable.end(), sortFunction);
  std::sort(localExpectedContent.begin(), localExpectedContent.end(),
            sortFunction);

  ASSERT_EQ(localTable, localExpectedContent);
}
