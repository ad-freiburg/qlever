// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/BenchmarkRecordToString.h"

// ___________________________________________________________________________
void addCategoryTitelToStringstream(std::stringstream* stringStream,
    const std::string categoryTitel){
  // The bar above and below the titel.
  const size_t barLength = categoryTitel.size() + 4;
  const std::string bar(barLength, '#');

  (*stringStream) << "\n" << bar << "\n# " << categoryTitel << " #\n" << bar << "\n";
}

// ___________________________________________________________________________
std::string recordEntryToString(const BenchmarkRecords::RecordEntry& recordEntry){
  return "'" + recordEntry.descriptor_ + "' took " +
    std::to_string(recordEntry.measuredTime_) + " seconds.";
}

// ___________________________________________________________________________
void addVectorOfRecordEntryToStringstream(std::stringstream* stringStream,
    const std::vector<BenchmarkRecords::RecordEntry>& entries,
    const std::string& prefix) {
  for (const BenchmarkRecords::RecordEntry& entry: entries) {
    (*stringStream) << "\n" << prefix << recordEntryToString(entry);
  }
};

// ___________________________________________________________________________
void addSingleMeasurementsToStringstream(std::stringstream* stringStream,
    const std::vector<BenchmarkRecords::RecordEntry>& recordEntries){
  addCategoryTitelToStringstream(stringStream, "Single measurment benchmarks");
  addVectorOfRecordEntryToStringstream(stringStream, recordEntries,
      "Single measurment benchmark ");
}

// ___________________________________________________________________________
void addGroupsToStringstream(std::stringstream* stringStream,
    const std::vector<BenchmarkRecords::RecordGroup>& recordGroups){
  addCategoryTitelToStringstream(stringStream, "Group benchmarks");
  for (const auto& group: recordGroups) {
    (*stringStream) << "\n\nGroup '" << group.descriptor_ << "':";
    addVectorOfRecordEntryToStringstream(stringStream, group.entries_, "\t");
  }
}

// ___________________________________________________________________________
void addTablesToStringstream(std::stringstream* stringStream,
    const std::vector<BenchmarkRecords::RecordTable>& recordTables){
  addCategoryTitelToStringstream(stringStream, "Table benchmarks");

  // For easier reading.
  using RecordTableEntryType = BenchmarkRecords::RecordTable::EntryType;

  // Used for the formating of numbers in the table.
  const size_t exactNumberOfDecimals = 4;

  // What should be printed between columns. Used for nicer formating.
  const std::string columnSeperator = " | ";

  // Convert an RecordTableEntryType to a screen friendly
  // format.
  auto recordTableEntryToStringVisitor = [&exactNumberOfDecimals]<typename T>(
      const T& entry)->std::string{
    // We may need formating.
    std::stringstream ss;

    // We have 3 possible types, because
    // RecordTableEntryType has three distinct possible
    // types, that all need different handeling.
    // Fortunaly, we can decide the handeling at compile time and throw the
    // others away, using `if constexpr(std::is_same<...,...>::eval`.
    if constexpr(std::is_same<T, std::monostate>::value) {
      // No value, print it as NA.
      ss << "NA";
    } else if constexpr(std::is_same<T, float>::value) {
      // There is a value, print it with exactNumberOfDecimals decimals.
      ss << std::fixed << std::setprecision(exactNumberOfDecimals) << entry;
    } else {
      // Only other possible type is a string, which needs no formating.
      ss << entry;
    }

    return ss.str();
  };
  auto recordTableEntryToString = [&exactNumberOfDecimals,
       &recordTableEntryToStringVisitor](const
      RecordTableEntryType& entry)->std::string{
    // The std::visit checks which type a std::variant is and calls our
    // visitor function with the right template parameter.
    return std::visit(recordTableEntryToStringVisitor, entry);
  };

  // Add a string to a stringstream with enough padding, empty spaces to the
  // right of the string, to reach the wanted length.
  auto addStringWithPadding = [](std::stringstream* stringStream,
      const auto& text, const size_t wantedLength){
    (*stringStream) << std::setw(wantedLength) << std::left << text;
  };

  // Printing the tables themselves.
  for (const auto& table: recordTables) {
    (*stringStream) << "\n\nTable '" << table.descriptor_ << "':\n\n";

    // For easier usage.
    size_t numberColumns = table.columnNames_.size();
    size_t numberRows = table.rowNames_.size();

    // For formating: What is the maximum string width of a column, if you
    // compare all it's entries?
    
    // The max width of the column with the row names.
    const size_t rowNameMaxStringWidth = std::ranges::max(table.rowNames_,
        {}, [](const std::string& name){return name.length();}).length();

    // The max width for columns with actual entries.
    std::vector<size_t> columnMaxStringWidth(numberColumns, 0);
    for (size_t column = 0; column < numberColumns; column++) {
      // Which of the entries is the longest?
      const std::vector<RecordTableEntryType>&
        rowWithTheWidestColumnEntry = std::ranges::max(table.entries_, {},
          [&column, &recordTableEntryToString](
            const std::vector<RecordTableEntryType>& row){
          return recordTableEntryToString(row[column]).length();});
      columnMaxStringWidth[column] =
        recordTableEntryToString(rowWithTheWidestColumnEntry[column]).length();
      // Is the name of the column bigger?
      columnMaxStringWidth[column] =
        (columnMaxStringWidth[column] > table.columnNames_[column].length()) ?
        columnMaxStringWidth[column] : table.columnNames_[column].length();
    }

    // Because the column of row names also has a width, we create an empty
    // string of that size, before actually printing the row of column names.
    (*stringStream) << std::string(rowNameMaxStringWidth, ' ');

    // Print the top row of names.
    for (size_t column = 0; column < numberColumns; column++){
      (*stringStream) << columnSeperator;
      addStringWithPadding(stringStream, table.columnNames_[column],
          columnMaxStringWidth[column]);
    }

    // Print the rows.
    for (size_t row = 0; row < numberRows; row++) {
      // Row name
      (*stringStream) << "\n";
      addStringWithPadding(stringStream, table.rowNames_[row],
          rowNameMaxStringWidth);

      // Row content.
      for (size_t column = 0; column < numberColumns; column++) {
        (*stringStream) << columnSeperator;
        addStringWithPadding(stringStream,
            recordTableEntryToString(table.entries_[row][column]),
            columnMaxStringWidth[column]);
      }
    }
  }
}

// ___________________________________________________________________________
std::string benchmarkRecordsToString(const BenchmarkRecords& records) {
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

  return visualization.str();
}
