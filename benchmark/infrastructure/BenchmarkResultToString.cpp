// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkResultToString.h"

// ___________________________________________________________________________
void addCategoryTitelToOStringstream(std::ostringstream* stream,
    std::string_view categoryTitel){
  // The bar above and below the titel.
  const size_t barLength = categoryTitel.size() + 4;
  const std::string bar(barLength, '#');

  (*stream) << "\n" << bar << "\n# " << categoryTitel << " #\n" << bar << "\n";
}

// ___________________________________________________________________________
void addVectorOfResultEntryToOStringstream(std::ostringstream* stream,
    const std::vector<BenchmarkMeasurementContainer::ResultEntry>& entries,
    const std::string& prefix) {
  for (const auto& entry: entries) {
    (*stream) << "\n" << prefix << (std::string)entry;
  }
};

// ___________________________________________________________________________
void addSingleMeasurementsToOStringstream(std::ostringstream* stream,
  const std::vector<BenchmarkMeasurementContainer::ResultEntry>&
  resultEntries){
  addCategoryTitelToOStringstream(stream, "Single measurment benchmarks");
  addVectorOfResultEntryToOStringstream(stream, resultEntries,
      "Single measurment benchmark ");
}

// ___________________________________________________________________________
void addGroupsToOStringstream(std::ostringstream* stream,
  const std::vector<BenchmarkMeasurementContainer::ResultGroup>& resultGroups){
  addCategoryTitelToOStringstream(stream, "Group benchmarks");
  for (const auto& group: resultGroups) {
    (*stream) << "\n\n" << (std::string)group;
  }
}

// ___________________________________________________________________________
void addTablesToOStringstream(std::ostringstream* stream,
  const std::vector<BenchmarkMeasurementContainer::ResultTable>& resultTables){
  addCategoryTitelToOStringstream(stream, "Table benchmarks");
  // Printing the tables themselves.
  for (const auto& table: resultTables) {
    (*stream) << "\n\n" << (std::string)table;
  }
}

// ___________________________________________________________________________
std::string benchmarkResultsToString(const BenchmarkResults& results) {
  // The values for all the categories of benchmarks.
  const std::vector<BenchmarkMeasurementContainer::ResultEntry>&
    singleMeasurements = results.getSingleMeasurements();
  const std::vector<BenchmarkMeasurementContainer::ResultGroup>&
    resultGroups = results.getGroups();
  const std::vector<BenchmarkMeasurementContainer::ResultTable>&
    resultTables = results.getTables();

  // Visualizes the measured times.
  std::ostringstream visualization;

  // @brief Adds a category to the string steam, if it is not empty. Mainly
  //  exists for reducing code duplication.
  //
  // @param stringStream The stringstream where the text will get added.
  // @param categoryResult The information needed, for printing the benchmark
  //  category. Should be a vector of ResultEntry, ResultGroup, or ResultTable.
  // @param categoryAddPrintStreamFunction The function, which given the
  //  benchmark category information, converts them to text, and adds that text
  //  to the stringstream.
  // @param suffix Added to the stringstream after
  //  categoryAddPrintStreamFunction. Mostly for linebreaks between those
  //  categories.
  auto addNonEmptyCategorieToStringSteam = [](std::ostringstream* stringStream,
      const auto& categoryResult, const auto& categoryAddPrintStreamFunction,
      const std::string& suffix = ""){
    if (categoryResult.size() > 0){
      categoryAddPrintStreamFunction(stringStream, categoryResult);
      (*stringStream) << suffix;
    }
  };

  // Visualization for single measurments, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, singleMeasurements,
      addSingleMeasurementsToOStringstream, "\n\n");

  // Visualization for groups, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, resultGroups,
      addGroupsToOStringstream, "\n\n");

  // Visualization for tables, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, resultTables,
      addTablesToOStringstream);

  return visualization.str();
}
