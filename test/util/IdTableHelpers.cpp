// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (Januar of 2023, schlegea@informatik.uni-freiburg.de)

#include "../test/util/IdTableHelpers.h"

#include <algorithm>
#include <utility>

#include "../engine/ValuesForTesting.h"
#include "engine/idTable/IdTable.h"
#include "global/ValueId.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/Forward.h"

// ____________________________________________________________________________
void compareIdTableWithExpectedContent(
    const IdTable& table, const IdTable& expectedContent,
    const bool resultMustBeSortedByJoinColumn, const size_t joinColumn,
    ad_utility::source_location l) {
  // For generating more informative messages, when failing the comparison.
  std::stringstream traceMessage{};

  auto writeIdTableToStream = [&traceMessage](const IdTable& idTable) {
    std::ranges::for_each(idTable,
                          [&traceMessage](const auto& row) {
                            // TODO<C++23> Use std::views::join_with for both
                            // loops.
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
  std::ranges::sort(localTable, std::ranges::lexicographical_compare);
  std::ranges::sort(localExpectedContent, std::ranges::lexicographical_compare);

  ASSERT_EQ(localTable, localExpectedContent);
}

// ____________________________________________________________________________
IdTable generateIdTable(
    const size_t numberRows, const size_t numberColumns,
    const std::function<std::vector<ValueId>()>& rowGenerator) {
  AD_CONTRACT_CHECK(numberRows > 0 && numberColumns > 0);

  // Creating the table and setting it to the wanted size.
  IdTable table{numberColumns, ad_utility::testing::makeAllocator()};
  table.resize(numberRows);

  // Fill the table.
  std::ranges::for_each(
      /*
      The iterator of an `IdTable` dereference to an `row_reference_restricted`,
      which only allows write access, if it is a r-value. Otherwise, we can't
      manipulate the content of the row.
      Which is why we have to use an universal reference, `AD_FWD` and also why
      we have to explicitly define the return type of the lambda.
      */
      table, [&rowGenerator, &numberColumns](auto&& row) -> void {
        // Make sure, that the generated row has the right
        // size, before using it.
        std::vector<ValueId> generatedRow = rowGenerator();
        AD_CONTRACT_CHECK(generatedRow.size() == numberColumns);

        std::ranges::copy(generatedRow, AD_FWD(row).begin());
      });

  return table;
}

// ____________________________________________________________________________
IdTable createRandomlyFilledIdTable(
    const size_t numberRows, const size_t numberColumns,
    const std::vector<std::pair<size_t, std::function<ValueId()>>>&
        joinColumnWithGenerator,
    const ad_utility::RandomSeed randomSeed) {
  AD_CONTRACT_CHECK(numberRows > 0 && numberColumns > 0);

  // Views for clearer access.
  auto joinColumnNumberView = std::views::keys(joinColumnWithGenerator);
  auto joinColumnGeneratorView = std::views::values(joinColumnWithGenerator);

  // Are all the join column numbers within the max column number?
  AD_CONTRACT_CHECK(std::ranges::all_of(
      joinColumnNumberView,
      [&numberColumns](const size_t num) { return num < numberColumns; }));

  // Are there no duplicates in the join column numbers?
  std::vector<size_t> sortedJoinColumnNumbers =
      ad_utility::transform(joinColumnNumberView, std::identity{});
  std::ranges::sort(sortedJoinColumnNumbers);
  AD_CONTRACT_CHECK(std::ranges::unique(sortedJoinColumnNumbers).empty());

  // Are all the functions for generating join column entries not nullptr?
  AD_CONTRACT_CHECK(std::ranges::all_of(
      joinColumnGeneratorView, [](auto func) { return func != nullptr; }));

  // The random number generators for normal entries.
  ad_utility::SlowRandomIntGenerator<size_t> randomNumberGenerator(
      0, ValueId::maxIndex, randomSeed);
  std::function<ValueId()> normalEntryGenerator = [&randomNumberGenerator]() {
    // `IdTable`s don't take raw numbers, you have to transform them first.
    return ad_utility::testing::VocabId(randomNumberGenerator());
  };

  // Assiging the column number to a generator function.
  std::vector<const std::function<ValueId()>*> columnToGenerator(
      numberColumns, &normalEntryGenerator);
  std::ranges::for_each(joinColumnWithGenerator,
                        [&columnToGenerator](auto& pair) {
                          columnToGenerator.at(pair.first) = &pair.second;
                        });

  // Creating the table.
  return generateIdTable(
      numberRows, numberColumns, [&numberColumns, &columnToGenerator]() {
        std::vector<ValueId> row(numberColumns);

        // Filling the row.
        for (size_t column = 0; column < numberColumns; column++) {
          row.at(column) = (*columnToGenerator.at(column))();
        }

        return row;
      });
}

// ____________________________________________________________________________
IdTable createRandomlyFilledIdTable(const size_t numberRows,
                                    const size_t numberColumns,
                                    const std::vector<size_t>& joinColumns,
                                    const std::function<ValueId()>& generator,
                                    const ad_utility::RandomSeed randomSeed) {
  // Is the generator not empty?
  AD_CONTRACT_CHECK(generator != nullptr);

  // Creating the table.
  return createRandomlyFilledIdTable(
      numberRows, numberColumns,
      ad_utility::transform(
          joinColumns,
          [&generator](const size_t num) {
            /*
            Simply passing `generator` doesn't work, because it would be copied,
            which would lead to different behavior. After all, the columns would
            no longer share a function with one internal state, but each have
            their own separate function with its own internal state.
            */
            return std::make_pair(
                num, std::function{[&generator]() { return generator(); }});
          }),
      randomSeed);
}

// ____________________________________________________________________________
IdTable createRandomlyFilledIdTable(
    const size_t numberRows, const size_t numberColumns,
    const std::vector<JoinColumnAndBounds>& joinColumnsAndBounds,
    const ad_utility::RandomSeed randomSeed) {
  // Entries in IdTables have a max size.
  constexpr size_t maxIdSize = ValueId::maxIndex;

  /*
  Is the lower bound smaller, or equal, to the upper bound? And is the upper
  bound smaller, or equal, to the maximum size of an IdTable entry?
  */
  AD_CONTRACT_CHECK(std::ranges::all_of(
      joinColumnsAndBounds, [](const JoinColumnAndBounds& j) {
        return j.lowerBound_ <= j.upperBound_ && j.upperBound_ <= maxIdSize;
      }));

  /*
  Instead of doing things ourselves, we will call the overload, who takes pairs
  of column indexes and functions.
  */
  const auto& joinColumnAndGenerator = ad_utility::transform(
      joinColumnsAndBounds, [](const JoinColumnAndBounds& j) {
        return std::make_pair(
            j.joinColumn_,
            // Each column gets its own random generator.
            std::function{
                [generator = ad_utility::SlowRandomIntGenerator<size_t>(
                     j.lowerBound_, j.upperBound_, j.randomSeed_)]() mutable {
                  // `IdTable`s don't take raw numbers, you have to transform
                  // them first.
                  return ad_utility::testing::VocabId(generator());
                }});
      });

  return createRandomlyFilledIdTable(numberRows, numberColumns,
                                     joinColumnAndGenerator, randomSeed);
}

// ____________________________________________________________________________
IdTable createRandomlyFilledIdTable(
    const size_t numberRows, const size_t numberColumns,
    const JoinColumnAndBounds& joinColumnAndBounds,
    const ad_utility::RandomSeed randomSeed) {
  // Just call the other overload.
  return createRandomlyFilledIdTable(
      numberRows, numberColumns, std::vector{joinColumnAndBounds}, randomSeed);
}

// ____________________________________________________________________________
IdTable createRandomlyFilledIdTable(const size_t numberRows,
                                    const size_t numberColumns,
                                    const ad_utility::RandomSeed randomSeed) {
  return createRandomlyFilledIdTable(numberRows, numberColumns,
                                     std::vector<JoinColumnAndBounds>{},
                                     randomSeed);
}

// ____________________________________________________________________________
std::shared_ptr<QueryExecutionTree> idTableToExecutionTree(
    QueryExecutionContext* qec, const IdTable& input) {
  size_t i = 0;
  auto v = [&i]() mutable { return Variable{"?" + std::to_string(i++)}; };
  std::vector<std::optional<Variable>> vars;
  std::generate_n(std::back_inserter(vars), input.numColumns(), std::ref(v));
  return ad_utility::makeExecutionTree<ValuesForTesting>(qec, input.clone(),
                                                         std::move(vars));
}
