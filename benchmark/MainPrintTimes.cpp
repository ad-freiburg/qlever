// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <iostream>
#include <ios>
#include <iomanip>
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
 * @brief Goes through all types of registered benchmarks, measures their time
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
  auto recordEntryToString = [](const BenchmarkRecords::RecordEntry& recordEntry)->std::string{
    return "'" + recordEntry.descriptor + "' took " +
      std::to_string(recordEntry.measuredTime) + " seconds.";
  };

  // Default way of adding a vector of RecordEntrys to a stringstream with
  // optional prefix.
  auto addVectorOfRecordEntry = [&recordEntryToString](
      std::stringstream* stringStream,
      const std::vector<BenchmarkRecords::RecordEntry>& entries,
      const std::string& prefix = "") {
    for (const BenchmarkRecords::RecordEntry& entry: entries) {
      (*stringStream) << "\n" << prefix << recordEntryToString(entry);
    }
  };

  // Visualization for single measurments.
  addCategoryTitelToStringstream(&visualization, "Single measurment benchmarks");
  addVectorOfRecordEntry(&visualization, records.getSingleMeasurements(),
      "Single measurment benchmark ");

  addCategoryBreak(&visualization);

  // Visualization for groups.
  addCategoryTitelToStringstream(&visualization, "Group benchmarks");
  for (const auto& entry: records.getGroups()) {
    const BenchmarkRecords::RecordGroup& group = entry.second;
    visualization << "\n\nGroup '" << group.descriptor << "':";
    addVectorOfRecordEntry(&visualization, group.entries, "\t");
  }

  addCategoryBreak(&visualization);

  // Visualization for tables.
  addCategoryTitelToStringstream(&visualization, "Table benchmarks");

  // Used for the formating of numbers in the table.
  const size_t exactNumberOfDecimals = 4;

  // What should be printed between columns. Used for nicer formating.
  const std::string columnSeperator = " | ";

  // Convert an std::optional<float> to a screen friendly format.
  auto optionalFloatToString = [&exactNumberOfDecimals](const
      std::optional<float>& optionalFloat)->std::string{
    // If there is no value, we print it as NA.
    if (!optionalFloat.has_value()) {return "NA";}

    // If there is a value, we print it with exactNumberOfDecimals
    // decimals.
    std::stringstream ss;
    ss << std::fixed << std::setprecision(exactNumberOfDecimals)
      << (optionalFloat.value());
    return ss.str();
  };

  // Add a string to a stringstream with enough padding, empty spaces to the
  // right of the string, to reach the wanted length.
  auto addStringWithPadding = [](std::stringstream* stringStream,
      const auto& text, const size_t wantedLength){
    (*stringStream) << std::setw(wantedLength) << std::left << text;
  };

  // Printing the tables themselves.
  for (const auto& entry: records.getTables()) {
    const BenchmarkRecords::RecordTable& table = entry.second;
    visualization << "\n\nTable '" << table.descriptor << "':\n\n";
  
    // For easier usage.
    size_t numberColumns = table.columnNames.size();
    size_t numberRows = table.rowNames.size();

    // For formating: What is the maximum string width of a column, if you
    // compare all it's entries?
    const size_t rowNameMaxStringWidth = std::ranges::max(table.rowNames,
        {}, [](const std::string& name){return name.length();}).length();

    std::vector<size_t> columnMaxStringWidth(numberColumns, 0);
    for (size_t column = 0; column < numberColumns; column++) {
      // Which of the entries is the longest?
      columnMaxStringWidth[column] = optionalFloatToString(
          std::ranges::max(table.entries, {},
          [&column, &optionalFloatToString](
            const std::vector<std::optional<float>>& row){
          return optionalFloatToString(row[column]).length();})[column]).length();
      // Is the name of the column bigger?
      columnMaxStringWidth[column] =
        (columnMaxStringWidth[column] > table.columnNames[column].length()) ?
        columnMaxStringWidth[column] : table.columnNames[column].length();
    }

    // Because the column of row names also has a width, we create an empty
    // string of that size, before actually printing the row of column names.
    visualization << std::string(rowNameMaxStringWidth, ' ');

    // Print the top row of names.
    for (size_t column = 0; column < numberColumns; column++){
      visualization << columnSeperator;
      addStringWithPadding(&visualization, table.columnNames[column], columnMaxStringWidth[column]);
    }

    // Print the rows.
    for (size_t row = 0; row < numberRows; row++) {
      // Row name
      visualization << "\n";
      addStringWithPadding(&visualization, table.rowNames[row], rowNameMaxStringWidth);
      
      // Row content.
      for (size_t column = 0; column < numberColumns; column++) {
        visualization << columnSeperator;
        addStringWithPadding(&visualization,
            optionalFloatToString(table.entries[row][column]), columnMaxStringWidth[column]);
      }
    }
  }

  std::cout << visualization.str() << "\n";
};
