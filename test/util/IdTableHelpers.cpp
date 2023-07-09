// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (Januar of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>

#include "../test/util/IdTableHelpers.h"

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
void sortIdTableByJoinColumnInPlace(IdTableAndJoinColumn& table) {
  CALL_FIXED_SIZE((std::array{table.idTable.numColumns()}), &Engine::sort,
                  &table.idTable, table.joinColumn);
}

// ____________________________________________________________________________
IdTable generateIdTable(
    const size_t numberRows, const size_t numberColumns,
    const std::function<IdTable::row_type()>& rowGenerator) {
  AD_CONTRACT_CHECK(numberRows > 0 && numberColumns > 0);

  // Creating the table and setting it to the wanted size.
  IdTable table{numberColumns, ad_utility::testing::makeAllocator()};
  table.resize(numberRows);

  // Fill the table.
  std::ranges::generate(table, [&numberColumns, &rowGenerator]() {
    // Make sure, that the generated row has the right size, before passing it
    // on.
    IdTable::row_type row = rowGenerator();

    if (row.numColumns() != numberColumns) {
      throw std::runtime_error(absl::StrCat(
          "The row generator generated a IdTable::row_type of size ",
          row.numColumns(), ", which is not the wanted size ", numberColumns,
          "."));
    }

    return row;
  });

  return table;
}

// ____________________________________________________________________________
IdTable createRandomlyFilledIdTable(
    const size_t numberRows, const size_t numberColumns,
    const std::vector<std::pair<size_t, std::function<ValueId()>>>&
        joinColumnWithGenerator) {
  AD_CONTRACT_CHECK(numberRows > 0 && numberColumns > 0);

  // Views for clearer access.
  auto joinColumnNumberView = std::views::keys(joinColumnWithGenerator);
  auto joinColumnGeneratorView = std::views::values(joinColumnWithGenerator);

  // Are all the join column numbers within the max column number?
  if (auto column = std::ranges::find_if(
          joinColumnNumberView,
          [&numberColumns](const size_t& num) { return num >= numberColumns; });
      column != joinColumnNumberView.end()) {
    throw std::runtime_error(
        absl::StrCat("The join columns", *column, " is out of bounds."));
  }

  // Are there no duplicates in the join column numbers?
  for (auto it = joinColumnNumberView.begin(); it != joinColumnNumberView.end();
       it++) {
    if (auto column = std::ranges::find_if(
            it + 1, joinColumnNumberView.end(),
            [&it](const size_t& num) { return *it == num; });
        column != joinColumnNumberView.end()) {
      throw std::runtime_error(
          absl::StrCat("Join column ", *column,
                       " got at least two generator assigned to it."));
    }
  }

  // Are all the functions for generating join column entries not nullptr?
  if (std::ranges::any_of(joinColumnGeneratorView,
                          [](auto func) { return func == nullptr; })) {
    throw std::runtime_error(
        "At least one of the generator function pointer was a nullptr.");
  }

  // The random number generators for normal entries.
  SlowRandomIntGenerator<size_t> randomNumberGenerator(0, ValueId::maxIndex);
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
        IdTable::row_type row(numberColumns);

        // Filling the row.
        for (size_t column = 0; column < numberColumns; column++) {
          row[column] = (*columnToGenerator.at(column))();
        }

        return row;
      });
}

// ____________________________________________________________________________
IdTable createRandomlyFilledIdTable(const size_t numberRows,
                                    const size_t numberColumns,
                                    const std::vector<size_t>& joinColumns,
                                    const std::function<ValueId()>& generator) {
  // Is the generator not empty?
  if (generator == nullptr) {
    throw std::runtime_error("The generator function was empty.");
  }

  // Creating the table.
  return createRandomlyFilledIdTable(
      numberRows, numberColumns,
      ad_utility::transform(joinColumns, [&generator](const size_t& num) {
        /*
        Simply passing `generator` doesn't work, because it would be copied,
        which would lead to different behavior. After all, the columns would no
        longer share a function with one internal state, but each have their own
        separate function with its own internal state.
        */
        return std::make_pair(
            num, std::function{[&generator]() { return generator(); }});
      }));
}

// ____________________________________________________________________________
IdTable createRandomlyFilledIdTable(const size_t numberRows,
                                    const size_t numberColumns,
                                    const size_t joinColumn,
                                    const size_t joinColumnLowerBound,
                                    const size_t joinColumnUpperBound) {
  // Entries in IdTables have a max size.
  constexpr size_t maxIdSize = ValueId::maxIndex;

  // Is the lower bound smaller, or equal, to the upper bound?
  AD_CONTRACT_CHECK(joinColumnLowerBound <= joinColumnUpperBound);
  // Is the upper bound smaller, or equal, to the maximum size of an IdTable
  // entry?
  AD_CONTRACT_CHECK(joinColumnUpperBound <= maxIdSize);

  // The random number generators for join column entries.
  SlowRandomIntGenerator<size_t> joinColumnEntryGenerator(joinColumnLowerBound,
                                                          joinColumnUpperBound);

  return createRandomlyFilledIdTable(
      numberRows, numberColumns, {joinColumn}, [&joinColumnEntryGenerator]() {
        // `IdTable`s don't take raw numbers, you have to transform them first.
        return ad_utility::testing::VocabId(joinColumnEntryGenerator());
      });
}
