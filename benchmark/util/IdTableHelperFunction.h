// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <cstdio>
#include <algorithm>

#include "engine/Engine.h"
#include "util/Random.h"
#include "util/Forward.h"
#include "engine/idTable/IdTable.h"
#include "../test/util/IdTableHelpers.h"

/*
 * @brief Return a IdTable, that is randomly filled. The range of numbers
 *  being entered in the join column can be defined.
 *
 * @param numberRows, numberColumns The size of the IdTable, that is to be
 *  returned.
 * @param joinColumn The joinColumn of the IdTable, that is to be returned.
 * @param joinColumnLowerBound, joinColumnUpperBound The range of the entries
 *  in the join column.
*/
IdTable createRandomlyFilledIdTable(const size_t numberRows,
    const size_t numberColumns, const size_t joinColumn,
    const size_t joinColumnLowerBound, const size_t joinColumnUpperBound) {
  // The random number generators for normal entries and join column entries.
  // Both can be found in util/Random.h
  SlowRandomIntGenerator<size_t> normalEntryGenerator(0, static_cast<size_t>(1ull << 59)); // Entries in IdTables have a max size.
  SlowRandomIntGenerator<size_t> joinColumnEntryGenerator(joinColumnLowerBound, joinColumnUpperBound);
  
  // There is a help function for creating IdTables by giving a vector of
  // vectores in EngineTest.cpp . So we define the content here, and it can
  // create it, because that is way easier.
  std::vector<std::vector<size_t>> tableContent(numberRows);

  // Fill tableContent with random content.
  for (size_t row = 0; row < numberRows; row++) {
    tableContent[row].resize(numberColumns);
    for (size_t i = 0; i < joinColumn; i++) {
      tableContent[row][i] = normalEntryGenerator();
    }
    tableContent[row][joinColumn] = joinColumnEntryGenerator();
    for (size_t i = joinColumn + 1; i < numberColumns; i++) {
      tableContent[row][i] = normalEntryGenerator();
    }
  }
  
  return makeIdTableFromVector(tableContent); 
}

