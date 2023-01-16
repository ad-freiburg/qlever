// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>

#include "../benchmark/Benchmark.h"
#include "../benchmark/util/MainFunctionHelperFunction.h"

/*
 * @brief Goes through all types of registerd benchmarks, measures their time
 * and prints their measured time in a fitting format.
 */
int main() {
  // The descriptors and measured times of all the benchmarks.
  const BenchmarkRecords records = measureTimeForAllBenchmarks(); 
 
  // Visualizes the measured times.
  std::stringstream visualization;

  // Visualization for single measurments.
  visualization << "################################\n"
                << "# Single measurment benchmarks #\n"
                << "################################\n";
  for (const BenchmarkRecords::RecordEntry& entry: records.getSingleMeasurments()) {
    visualization << "\nSingle measurment benchmark '" << entry.descriptor
                  << "' took " << entry.measuredTime << " seconds.";
  }

  visualization << "\n\n";

  // Visualization for groups.
  visualization << "####################\n"
                << "# Group benchmarks #\n"
                << "####################";
  for (const auto& entry: records.getGroups()) {
    const BenchmarkRecords::RecordGroup& group = entry.second;
    visualization << "\n\nGroup '" << group.descriptor << "':";
    for (const BenchmarkRecords::RecordEntry& groupEntry: group.entries) {
      visualization << "\n\t'" << groupEntry.descriptor << "' took "
                    << groupEntry.measuredTime << " seconds.";
    }
  }

  // Visualization for tables.
  visualization << "####################\n"
                << "# Table benchmarks #\n"
                << "####################";
  for (const auto& entry: records.getTables()) {
    const BenchmarkRecords::RecordTable& table = entry.second;
    visualization << "\n\nTable '" << table.descriptor << "':\n\n";

    // For easier iteration of table entries.
    size_t numberColumns = table.columnNames.size();
    size_t numberRows = table.rowNames.size();

    // Print the top row of names, before doing anything else.
    for (const std::string& columnName: table.columnNames) {
      visualization << columnName << "\t";
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
