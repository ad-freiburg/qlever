// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/string_view.h>
#include <memory>
#include <sstream>
#include <algorithm>
#include <string_view>

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkResultToString.h"
#include "BenchmarkMetadata.h"
#include "util/Algorithm.h"

namespace ad_benchmark{

// ____________________________________________________________________________
BenchmarkMetadata& BenchmarkMetadataGetter::getMetadata(){
  return metadata_;
}

// ____________________________________________________________________________
const BenchmarkMetadata& BenchmarkMetadataGetter::getMetadata() const{
  return metadata_;
}

// ____________________________________________________________________________
ResultEntry::operator std::string() const{
  return absl::StrCat("'", descriptor_, "' took ", measuredTime_, " seconds.");
}

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const ResultEntry& resultEntry){
  j = nlohmann::json{{"descriptor", resultEntry.descriptor_},
    {"measuredTime", resultEntry.measuredTime_},
    {"metadata", resultEntry.getMetadata()}};
}

// ____________________________________________________________________________
ResultGroup::operator std::string() const{
  // We need to add all the string representations of the group members,
  // so doing everything using a stream is the best idea.
  std::ostringstream stream;

  // The normal foreword.
  stream << "Group '" << descriptor_ << "':";

  // Listing all the entries.
  addVectorOfResultEntryToOStringstream(&stream, ad_utility::transform(entries_,
    [](const auto& pointer){return (*pointer);}), "\t");

  return stream.str();
}

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const ResultGroup& resultGroup){
  j = nlohmann::json{{"descriptor", resultGroup.descriptor_},
    {"entries", resultGroup.entries_},
    {"metadata", resultGroup.getMetadata()}};
}

// ____________________________________________________________________________
void ResultTable::setEntry(const size_t& row, const size_t& column,
  const EntryType& newEntryContent){
  entries_.at(row).at(column) = newEntryContent;
}

// ____________________________________________________________________________
ResultTable::operator std::string() const{
  // Used for the formating of numbers in the table. They will always be
  // formated as having 4 values after the decimal point.
  constexpr absl::string_view floatFormatSpecifier = "%.4f";

  // What should be printed between columns. Used for nicer formating.
  const std::string columnSeperator = " | ";

  // Convert an `EntryType` of `ResultTable` to a screen friendly
  // format.
  auto entryToStringVisitor = [&floatFormatSpecifier]<typename T>(
      const T& entry){
    // We have 3 possible types, because `EntryType` has three distinct possible
    // types, that all need different handeling.
    // Fortunaly, we can decide the handeling at compile time and throw the
    // others away, using `if constexpr(std::is_same<...,...>::value)`.
    if constexpr(std::is_same<T, std::monostate>::value) {
      // No value, print it as NA.
      return (std::string)"NA";
    } else if constexpr(std::is_same<T, float>::value) {
      // There is a value, format it as specified.
      return absl::StrFormat(floatFormatSpecifier, entry);
    } else {
      // Only other possible type is a string, which needs no formating.
      return entry;
    }
  };
  auto entryToString = [&entryToStringVisitor](const
      EntryType& entry){
    // The std::visit checks which type a std::variant is and calls our
    // visitor function with the right template parameter.
    return std::visit(entryToStringVisitor, entry);
  };

  /*
  Add a string to a stringstream with enough padding, empty spaces to the
  right of the string, to reach the wanted length. Doesn't shorten the
  given string.
  */
  auto addStringWithPadding = [](std::ostringstream& stream,
      const std::string& text, const size_t wantedLength){
    const std::string padding(wantedLength - text.length(), ' ');
    stream << text << padding;
  };

  // For printing the table.
  std::ostringstream stream;

  // Adding the table to the stream.
  stream << "Table '" << descriptor_ << "':\n\n";

  // For easier usage.
  const size_t numberColumns = columnNames_.size();
  const size_t numberRows = rowNames_.size();

  // For formating: What is the maximum string width of a column, if you
  // compare all it's entries?
  
  // The max width of the column containing the row names.
  const size_t rowNameMaxStringWidth = std::ranges::max(rowNames_,
      {}, [](std::string_view name){return name.length();}).length();

  // The max width for columns with actual entries.
  std::vector<size_t> columnMaxStringWidth(numberColumns, 0);
  for (size_t column = 0; column < numberColumns; column++) {
    // Which of the entries is the longest?
    const std::vector<EntryType>&
      rowWithTheWidestColumnEntry = std::ranges::max(entries_, {},
        [&column, &entryToString](
          const std::vector<EntryType>& row){
        return entryToString(row[column]).length();});
    columnMaxStringWidth[column] =
      entryToString(rowWithTheWidestColumnEntry[column]).length();
    // Is the name of the column bigger?
    columnMaxStringWidth[column] =
      (columnMaxStringWidth[column] > columnNames_[column].length()) ?
      columnMaxStringWidth[column] : columnNames_[column].length();
  }

  // Because the column of row names also has a width, we create an empty
  // string of that size, before actually printing the row of column names.
  stream << std::string(rowNameMaxStringWidth, ' ');

  // Print the top row of names.
  for (size_t column = 0; column < numberColumns; column++){
    stream << columnSeperator;
    addStringWithPadding(stream, columnNames_[column],
        columnMaxStringWidth[column]);
  }

  // Print the rows.
  for (size_t row = 0; row < numberRows; row++) {
    // Row name
    stream << "\n";
    addStringWithPadding(stream, rowNames_[row],
        rowNameMaxStringWidth);

    // Row content.
    for (size_t column = 0; column < numberColumns; column++) {
      stream << columnSeperator;
      addStringWithPadding(stream,
          entryToString(entries_[row][column]),
          columnMaxStringWidth[column]);
    }
  }

  return stream.str();
}

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const ResultTable& resultTable){
  j = nlohmann::json{{"descriptor", resultTable.descriptor_},
    {"rowNames", resultTable.rowNames_},
    {"columnNames", resultTable.columnNames_},
    {"entries", resultTable.entries_},
    {"metadata", resultTable.getMetadata()}};
}

} // End of namespace `ad_benchmark`
