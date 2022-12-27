// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)
// Author of the file this file is based on: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <tuple>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/OptionalJoin.h"
#include "util/Random.h"
#include "engine/QueryExecutionTree.h"
#include "util/Forward.h"
#include "util/SourceLocation.h"
#include "engine/idTable/IdTable.h"
#include "../benchmark/Benchmark.h"

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
 * Return an 'IdTable' with the given 'tableContent'. all rows must have the
 * same length.
*/
IdTable makeIdTableFromVector(std::vector<std::vector<size_t>> tableContent) {
  IdTable result(tableContent[0].size(), allocator());

  // Copying the content into the table.
  for (const auto& row: tableContent) {
    const size_t backIndex = result.size();
    result.emplace_back();

    for (size_t c = 0; c < tableContent[0].size(); c++) {
      result(backIndex, c) = I(row[c]);
    }
  }

  return result;
}

/*
 * @brief Joins two IdTables togehter with the given join function and returns
 * the result.
 *
 * @tparam JOIN_FUNCTION is used to allow the transfer of any type of
 *  lambda function, that could contain a join function. You never have to
 *  actually specify this parameter , just let inference do its job.
 *
 * @param tableA, tableB the tables. 
 * @param jcA, jcB are the join columns for the tables.
 * @param func the function, that will be used for joining the two tables
 *  together. Look into src/engine/Join.h for how it should look like.
 *
 * @returns tableA and tableB joined together in a IdTable.
 */
template<typename JOIN_FUNCTION>
IdTable useJoinFunctionOnIdTables(
        const IdTable& tableA,
        const size_t jcA,
        const IdTable& tableB,
        const size_t jcB,
        JOIN_FUNCTION func
        ) {
  
  int reswidth = tableA.numColumns() + tableB.numColumns()  - 1;
  IdTable res(reswidth, allocator());
 
  // You need to use this special function for executing lambdas. The normal
  // function for functions won't work.
  // Additionaly, we need to cast the two size_t, because callFixedSize only takes arrays of int.
  ad_utility::callFixedSize((std::array{static_cast<int>(tableA.numColumns()), static_cast<int>(tableB.numColumns()), reswidth}), func, tableA, jcA, tableB, jcB, &res);

  return res;
}

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

// After this point you can find some hard coded benchmarks.

void BM_RandomFillTable(BenchmarkRecords* records) {
  Join J{Join::InvalidOnlyForTestingJoinTag{}};
  auto JoinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    return J.join<A, B, C>(AD_FWD(args)...);
  };

  IdTable a = createRandomlyFilledIdTable(1000, 1000, 0, 0, 50);
  IdTable b = createRandomlyFilledIdTable(1000, 1000, 0, 0, 50);
  
  records->measureTime("Normal join with randomly filled IdTables", [&]() {
        useJoinFunctionOnIdTables(a, 0, b, 0, JoinLambda);
      }
    );
}

BenchmarkRegister temp1{{&BM_RandomFillTable}};
