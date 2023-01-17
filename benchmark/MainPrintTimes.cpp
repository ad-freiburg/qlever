// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>

#include "../benchmark/Benchmark.h"
#include "../benchmark/util/MainFunctionHelperFunction.h"

// These helper function are only usable inside this file. Should you want to
// use them elsewhere, please move them to a helper file in util, to prevent
// code duplication.
namespace {

/*
 * @brief Add a string of the form
 * "
 *  #################
 *  # categoryTitel #
 *  #################
 *
 *  "
 *  to the stringStream.
 */
void addCategoryTitelToStringstream(std::stringstream* stringStream,
    const std::string categoryTitel){
  // The bar above and below the titel.
  const size_t barLength = categoryTitel.size() + 4;
  const std::string bar(barLength, '#');

  (*stringStream) << "\n" << bar << "\n# " << categoryTitel << " #\n" << bar << "\n";
}

}

/*
 * @brief Goes through all types of registerd benchmarks, measures their time
 * and prints their measured time in a fitting format.
 */
int main() {
  // The descriptors and measured times of all the benchmarks.
  const BenchmarkRecords records = measureTimeForAllBenchmarks(); 
 
  // Visualizes the measured times.
  std::stringstream visualization;

  // For minimizing code duplication.
  // For adding linebreaks between printed categories.
  auto addCategoryBreak = [](std::stringstream* stringStream){(*stringStream) << "\n\n";};

  // Default conversion from BenchmarkRecords::RecordEntry to string.
  auto recordEntryToString = [](const BenchmarkRecords::RecordEntry& recordEntry){
    return "'" + recordEntry.descriptor + "' took " +
      std::to_string(recordEntry.measuredTime) + " seconds.";
  };

  // Visualization for single measurments.
  addCategoryTitelToStringstream(&visualization, "Single measurment benchmarks");
  for (const BenchmarkRecords::RecordEntry& entry: records.getSingleMeasurments()) {
    visualization << "\nSingle measurment benchmark " << recordEntryToString(entry);
  }

  addCategoryBreak(&visualization);

  // Visualization for groups.
  addCategoryTitelToStringstream(&visualization, "Group benchmarks");
  for (const auto& entry: records.getGroups()) {
    const BenchmarkRecords::RecordGroup& group = entry.second;
    visualization << "\n\nGroup '" << group.descriptor << "':";
    for (const BenchmarkRecords::RecordEntry& groupEntry: group.entries) {
      visualization << "\n\t" << recordEntryToString(groupEntry);
    }
  }

  addCategoryBreak(&visualization);

  // Visualization for tables.
  addCategoryTitelToStringstream(&visualization, "Table benchmarks");
  for (const auto& entry: records.getTables()) {
    const BenchmarkRecords::RecordTable& table = entry.second;
    visualization << "\n\nTable '" << table.descriptor << "':\n\n";

    // For easier iteration of table entries.
    size_t numberColumns = table.columnNames.size();
    size_t numberRows = table.rowNames.size();

    // Print the top row of names, before doing anything else.
    for (const std::string& columnName: table.columnNames) {
      visualization << "\t" << columnName;
    }

    // Print the rows.
    for (size_t row = 0; row < numberRows; row++) {
      visualization << "\n" << table.rowNames[row] << "\t";
      for (size_t column = 0; column < numberColumns; column++) {
        visualization << table.entries[row][column] << "\t";
      }
    }
  }

  std::cout << visualization.str() << "\n";
};
