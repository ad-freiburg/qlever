// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/string_view.h>

#include <algorithm>
#include <memory>
#include <sstream>
#include <string_view>
#include <utility>

#include "../benchmark/infrastructure/BenchmarkResultToString.h"
#include "BenchmarkMetadata.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/Iterators.h"

namespace ad_benchmark {

// ____________________________________________________________________________
BenchmarkMetadata& BenchmarkMetadataGetter::metadata() { return metadata_; }

// ____________________________________________________________________________
const BenchmarkMetadata& BenchmarkMetadataGetter::metadata() const {
  return metadata_;
}

// ____________________________________________________________________________
ResultEntry::operator std::string() const {
  return absl::StrCat(
      "Single measurement '", descriptor_, "'\n",
      addIndentation(
          absl::StrCat(getMetadataPrettyString(metadata(), "metadata: ", "\n"),
                       "time: ", measuredTime_, "s"),
          1));
}

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const ResultEntry& resultEntry) {
  j = nlohmann::json{{"descriptor", resultEntry.descriptor_},
                     {"measuredTime", resultEntry.measuredTime_},
                     {"metadata", resultEntry.metadata()}};
}

// ____________________________________________________________________________
ResultTable& ResultGroup::addTable(
    const std::string& descriptor, const std::vector<std::string>& rowNames,
    const std::vector<std::string>& columnNames) {
  resultTables_.push_back(ad_utility::make_copyable_unique<ResultTable>(
      descriptor, rowNames, columnNames));
  return (*resultTables_.back());
}

// ____________________________________________________________________________
ResultGroup::operator std::string() const {
  // The prefix. Everything after this will be indented, so it's better
  // to only combine them at the end.
  std::string prefix = absl::StrCat("Group '", descriptor_, "'\n");

  // We need to add all the string representations of the group members,
  // so  using a stream is the best idea.
  std::ostringstream stream;

  /*
  If the given vector is empty, add ` None` to the stream. If it isn't, adds
  "\n\n" and then calls the given function.
  */
  auto addWithFunctionOrNone = [&stream](const auto& vec, auto func) {
    if (!vec.empty()) {
      stream << "\n\n";
      func();
    } else {
      stream << " None";
    }
  };

  stream << absl::StrCat(
      getMetadataPrettyString(metadata(), "metadata: ", "\n"), "Measurements:");

  // Listing all the `ResultEntry`s, if there are any.
  addWithFunctionOrNone(resultEntries_, [&stream, this]() {
    addVectorOfResultEntryToOStringstream(
        &stream,
        ad_utility::transform(resultEntries_,
                              [](const auto& pointer) { return (*pointer); }),
        std::string{outputIndentation}, std::string{outputIndentation});
  });

  // Listing all the `ResultTable`s, if there are any.
  stream << "\n\nTables:";
  addWithFunctionOrNone(resultTables_, [&stream, this]() {
    stream << addIndentation(
        vectorOfResultTableToListString(ad_utility::transform(
            resultTables_, [](const auto& pointer) { return (*pointer); })),
        1);
  });

  return absl::StrCat(prefix, addIndentation(stream.str(), 1));
}

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const ResultGroup& resultGroup) {
  j = nlohmann::json{{"descriptor", resultGroup.descriptor_},
                     {"resultEntries", resultGroup.resultEntries_},
                     {"resultTables", resultGroup.resultTables_},
                     {"metadata", resultGroup.metadata()}};
}

// ____________________________________________________________________________
ResultTable::ResultTable(const std::string& descriptor,
                         const std::vector<std::string>& rowNames,
                         const std::vector<std::string>& columnNames)
    : descriptor_{descriptor},
      columnNames_{columnNames},
      entries_(rowNames.size(), std::vector<EntryType>(columnNames.size())) {
  // Having a table without any columns makes no sense.
  if (columnNames.empty()) {
    throw ad_utility::Exception(
        absl::StrCat("A `ResultTable` must have at"
                     " least one column. Table '",
                     descriptor, "' has ", columnNames.size(), " columns"));
  }

  // Setting the row names.
  for (size_t row = 0; row < rowNames.size(); row++) {
    setEntry(row, 0, rowNames.at(row));
  }
}

// ____________________________________________________________________________
void ResultTable::setEntry(const size_t& row, const size_t& column,
                           const EntryType& newEntryContent) {
  entries_.at(row).at(column) = newEntryContent;
}

// ____________________________________________________________________________
ResultTable::operator std::string() const {
  // Used for the formating of numbers in the table. They will always be
  // formated as having 4 values after the decimal point.
  static constexpr absl::string_view floatFormatSpecifier = "%.4f";

  // What should be printed between columns. Used for nicer formating.
  const std::string columnSeperator = " | ";

  // Convert an `EntryType` of `ResultTable` to a screen friendly
  // format.
  auto entryToStringVisitor = []<typename T>(const T& entry) {
    // We have 3 possible types, because `EntryType` has three distinct possible
    // types, that all need different handeling.
    // Fortunaly, we can decide the handeling at compile time and throw the
    // others away, using `if constexpr(std::is_same<...,...>::value)`.
    if constexpr (std::is_same_v<T, std::monostate>) {
      // No value, print it as NA.
      return (std::string) "NA";
    } else if constexpr (std::is_same_v<T, float>) {
      // There is a value, format it as specified.
      return absl::StrFormat(floatFormatSpecifier, entry);
    } else {
      // Only other possible type is a string, which needs no formating.
      return entry;
    }
  };
  auto entryToString = [&entryToStringVisitor](const EntryType& entry) {
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
                                 const std::string& text,
                                 const size_t wantedLength) {
    const std::string padding(
        wantedLength >= text.length() ? wantedLength - text.length() : 0, ' ');
    stream << text << padding;
  };

  /*
  @brief Adds a row of the table to the stream.

  @param stream The string stream, it will add to.
  @param rowEntries The entries for the rows. The `size_t` says, how long the
  printed string should be and the string is the string, that will printed.
  */
  auto addRow =
      [&columnSeperator, &addStringWithPadding](
          std::ostringstream& stream,
          const std::vector<std::pair<std::string, size_t>>& rowEntries) {
        forEachExcludingTheLastOne(
            rowEntries,
            [&stream, &columnSeperator,
             &addStringWithPadding](const auto& pair) {
              addStringWithPadding(stream, pair.first, pair.second);
              stream << columnSeperator;
            },
            [&stream, &addStringWithPadding](const auto& pair) {
              addStringWithPadding(stream, pair.first, pair.second);
            });
      };

  // The prefix. Everything after this will be indented, so it's better
  // to only combine them at the end.
  std::string prefix = absl::StrCat("Table '", descriptor_, "'\n");

  // For printing the table.
  std::ostringstream stream;

  // Adding the metadata.
  stream << getMetadataPrettyString(metadata(), "metadata: ", "\n");

  // It's allowed to have tables without rows. In that case, we are already
  // nearly done, cause we only to have add the column names.
  if (numRows() == 0) {
    // Adding the column names. We don't need any padding.
    addRow(stream, ad_utility::transform(columnNames_, [](const auto& name) {
             return std::make_pair(name, name.length());
           }));

    // Signal, that the table is empty.
    stream << "\n## Empty Table (0 rows) ##";

    return absl::StrCat(prefix, addIndentation(stream.str(), 1));
  }

  // For easier usage.
  const size_t numberColumns = numColumns();
  const size_t numberRows = numRows();

  // For formating: What is the maximum string width of a column, if you
  // compare all it's entries?
  std::vector<size_t> columnMaxStringWidth(numberColumns, 0);
  for (size_t column = 0; column < numberColumns; column++) {
    // Which of the entries is the longest?
    const std::vector<EntryType>& rowWithTheWidestColumnEntry =
        std::ranges::max(
            entries_, {},
            [&column, &entryToString](const std::vector<EntryType>& row) {
              return entryToString(row[column]).length();
            });
    columnMaxStringWidth[column] =
        entryToString(rowWithTheWidestColumnEntry[column]).length();
    // Is the name of the column bigger?
    columnMaxStringWidth[column] =
        (columnMaxStringWidth[column] > columnNames_[column].length())
            ? columnMaxStringWidth[column]
            : columnNames_[column].length();
  }

  // Print the top row of names.
  addRow(stream, ad_utility::zipVectors(columnNames_, columnMaxStringWidth));

  // Print the rows.
  for (size_t row = 0; row < numberRows; row++) {
    // Line break between rows.
    stream << "\n";

    // Actually printing the row.
    addRow(stream, ad_utility::zipVectors(
                       ad_utility::transform(entries_.at(row), entryToString),
                       columnMaxStringWidth));
  }

  return absl::StrCat(prefix, addIndentation(stream.str(), 1));
}

// ____________________________________________________________________________
void ResultTable::addRow() {
  // Create an emptry row of the same size as every other row.
  entries_.emplace_back(numColumns());
}

// ____________________________________________________________________________
size_t ResultTable::numRows() const { return entries_.size(); }

// ____________________________________________________________________________
size_t ResultTable::numColumns() const {
  /*
  If nobody played around with the private member variables, the amount of
  columns and column names should be the same.
  */
  return columnNames_.size();
}

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const ResultTable& resultTable) {
  j = nlohmann::json{{"descriptor", resultTable.descriptor_},
                     {"columnNames", resultTable.columnNames_},
                     {"entries", resultTable.entries_},
                     {"metadata", resultTable.metadata()}};
}

}  // namespace ad_benchmark
