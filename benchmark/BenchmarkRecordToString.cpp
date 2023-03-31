// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/BenchmarkRecordToString.h"

// ___________________________________________________________________________
void addCategoryTitelToOStringstream(std::ostringstream* stream,
    const std::string categoryTitel){
  // The bar above and below the titel.
  const size_t barLength = categoryTitel.size() + 4;
  const std::string bar(barLength, '#');

  (*stream) << "\n" << bar << "\n# " << categoryTitel << " #\n" << bar << "\n";
}

// ___________________________________________________________________________
void addVectorOfRecordEntryToOStringstream(std::ostringstream* stream,
    const std::vector<BenchmarkMeasurementContainer::RecordEntry>& entries,
    const std::string& prefix) {
  for (const auto& entry: entries) {
    (*stream) << "\n" << prefix << (std::string)entry;
  }
};

// ___________________________________________________________________________
void addSingleMeasurementsToOStringstream(std::ostringstream* stream,
  const std::vector<BenchmarkMeasurementContainer::RecordEntry>&
  recordEntries){
  addCategoryTitelToOStringstream(stream, "Single measurment benchmarks");
  addVectorOfRecordEntryToOStringstream(stream, recordEntries,
      "Single measurment benchmark ");
}

// ___________________________________________________________________________
void addGroupsToOStringstream(std::ostringstream* stream,
  const std::vector<BenchmarkMeasurementContainer::RecordGroup>& recordGroups){
  addCategoryTitelToOStringstream(stream, "Group benchmarks");
  for (const auto& group: recordGroups) {
    (*stream) << "\n\n" << (std::string)group;
  }
}

// ___________________________________________________________________________
void addTablesToOStringstream(std::ostringstream* stream,
  const std::vector<BenchmarkMeasurementContainer::RecordTable>& recordTables){
  addCategoryTitelToOStringstream(stream, "Table benchmarks");
  // Printing the tables themselves.
  for (const auto& table: recordTables) {
    (*stream) << "\n\n" << (std::string)table;
  }
}

// ___________________________________________________________________________
std::string benchmarkRecordsToString(const BenchmarkRecords& records) {
  // The values for all the categories of benchmarks.
  const std::vector<BenchmarkMeasurementContainer::RecordEntry>&
    singleMeasurements = records.getSingleMeasurements();
  const std::vector<BenchmarkMeasurementContainer::RecordGroup>&
    recordGroups = records.getGroups();
  const std::vector<BenchmarkMeasurementContainer::RecordTable>&
    recordTables = records.getTables();

  // Visualizes the measured times.
  std::ostringstream visualization;

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
  auto addNonEmptyCategorieToStringSteam = [](std::ostringstream* stringStream,
      const auto& categoryRecord, const auto& categoryAddPrintStreamFunction,
      const std::string& suffix = ""){
    if (categoryRecord.size() > 0){
      categoryAddPrintStreamFunction(stringStream, categoryRecord);
      (*stringStream) << suffix;
    }
  };

  // Visualization for single measurments, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, singleMeasurements,
      addSingleMeasurementsToOStringstream, "\n\n");

  // Visualization for groups, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, recordGroups,
      addGroupsToOStringstream, "\n\n");

  // Visualization for tables, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, recordTables,
      addTablesToOStringstream);

  return visualization.str();
}
