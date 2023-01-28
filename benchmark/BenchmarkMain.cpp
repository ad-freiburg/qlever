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

// Default conversion from BenchmarkRecords::RecordEntry to string.
std::string recordEntryToString(const BenchmarkRecords::RecordEntry& recordEntry){
  return "'" + recordEntry.descriptor + "' took " +
    std::to_string(recordEntry.measuredTime) + " seconds.";
}

// Default way of adding a vector of RecordEntrys to a stringstream with
// optional prefix.
void addVectorOfRecordEntryToStrinstream(std::stringstream* stringStream,
    const std::vector<BenchmarkRecords::RecordEntry>& entries,
    const std::string& prefix = "") {
  for (const BenchmarkRecords::RecordEntry& entry: entries) {
    (*stringStream) << "\n" << prefix << recordEntryToString(entry);
  }
};

// Visualization for single measurments.
void addSingleMeasurementsToStringstream(std::stringstream* stringStream,
    const std::vector<BenchmarkRecords::RecordEntry>& recordEntries){
  addCategoryTitelToStringstream(stringStream, "Single measurment benchmarks");
  addVectorOfRecordEntryToStrinstream(stringStream, recordEntries,
      "Single measurment benchmark ");
}

// Visualization for groups.
void addGroupsToStringstream(std::stringstream* stringStream,
    const std::vector<BenchmarkRecords::RecordGroup>& recordGroups){
  addCategoryTitelToStringstream(stringStream, "Group benchmarks");
  for (const auto& group: recordGroups) {
    (*stringStream) << "\n\nGroup '" << group.descriptor << "':";
    addVectorOfRecordEntryToStrinstream(stringStream, group.entries, "\t");
  }
}

// Visualization for tables.
void addTablesToStringstream(std::stringstream* stringStream,
    const std::vector<BenchmarkRecords::RecordTable>& recordTables){
  addCategoryTitelToStringstream(stringStream, "Table benchmarks");

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
  for (const auto& table: recordTables) {
    (*stringStream) << "\n\nTable '" << table.descriptor << "':\n\n";
  
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
    (*stringStream) << std::string(rowNameMaxStringWidth, ' ');

    // Print the top row of names.
    for (size_t column = 0; column < numberColumns; column++){
      (*stringStream) << columnSeperator;
      addStringWithPadding(stringStream, table.columnNames[column],
          columnMaxStringWidth[column]);
    }

    // Print the rows.
    for (size_t row = 0; row < numberRows; row++) {
      // Row name
      (*stringStream) << "\n";
      addStringWithPadding(stringStream, table.rowNames[row],
          rowNameMaxStringWidth);
      
      // Row content.
      for (size_t column = 0; column < numberColumns; column++) {
        (*stringStream) << columnSeperator;
        addStringWithPadding(stringStream,
            optionalFloatToString(table.entries[row][column]),
            columnMaxStringWidth[column]);
      }
    }
  }
}

void printBenchmarkRecords(const BenchmarkRecords& records) {
  // The values for all the categories of benchmarks.
  const std::vector<BenchmarkRecords::RecordEntry>& singleMeasurements =
    records.getSingleMeasurements();
  const std::vector<BenchmarkRecords::RecordGroup>& recordGroups =
    records.getGroups();
  const std::vector<BenchmarkRecords::RecordTable>& recordTables =
    records.getTables();

  // Visualizes the measured times.
  std::stringstream visualization;
  
  // @brief Adds a category to the string steam, if it is not empty. Mainly
  //  exists for reducing code duplication.
  //
  // @param stringStream The stringstream where the text will get added.
  // @param categoryRecord The information needed, for printing the benchmark
  //  category. Should be a vector of RecordEntry, RecordGroup, or RecordTable.
  // @param categoryAddPrintStreamFunction The function, which given the
  //  benchmark category information, converts them to text, and adds that text
  //  to the stringstream.
  // @param suffix Added to the stringstream after
  //  categoryAddPrintStreamFunction. Mostly for linebreaks between those
  //  categories.
  auto addNonEmptyCategorieToStringSteam = [](std::stringstream* stringStream,
      const auto& categoryRecord, const auto& categoryAddPrintStreamFunction,
      const std::string& suffix = ""){
    if (categoryRecord.size() > 0){
      categoryAddPrintStreamFunction(stringStream, categoryRecord);
      (*stringStream) << suffix;
    }
  };

  // Visualization for single measurments, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, singleMeasurements,
      addSingleMeasurementsToStringstream, "\n\n");

  // Visualization for groups, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, recordGroups,
      addGroupsToStringstream, "\n\n");

  // Visualization for tables, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, recordTables,
      addTablesToStringstream);

  std::cout << visualization.str() << "\n";
}

}

/*
 * @brief Goes through all types of registered benchmarks, measures their time
 * and prints their measured time in a fitting format.
 */
int main() {
  // Measuering the time for all registered benchmarks.
  // For measuring and saving the times.
  BenchmarkRecords records;
 
  // Goes through all registered benchmarks and measures them.
  for (const auto& benchmarkFunction: BenchmarkRegister::getRegisteredBenchmarks()) {
    benchmarkFunction(&records);
  }

  printBenchmarkRecords(records);
}
