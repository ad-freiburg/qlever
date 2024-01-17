// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/string_view.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "../benchmark/infrastructure/BenchmarkToString.h"
#include "BenchmarkMetadata.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/Iterators.h"
#include "util/StringUtils.h"

using namespace std::string_literals;

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
      ad_utility::addIndentation(
          absl::StrCat(getMetadataPrettyString(metadata(), "metadata: ", "\n"),
                       "time: ", measuredTime_, "s"),
          "    "));
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
      descriptor, absl::StrCat(descriptor, " of group ", descriptor_), rowNames,
      columnNames));
  return (*resultTables_.back());
}

// ____________________________________________________________________________
ResultGroup::operator std::string() const {
  // We need to add all the string representations of the group members,
  // so  using a stream is the best idea.
  std::ostringstream stream;

  /*
  If the given vector is empty, return " None". Else, return the concatenation
  of "\n\n" with the string list representation of the vector.
  */
  auto vectorToStringListOrNone =
      []<typename T>(const std::vector<T>& vec) -> std::string {
    if (vec.empty()) {
      return " None";
    }

    const std::string& list = ad_utility::lazyStrJoin(
        std::views::transform(
            vec,
            [](const auto& pointer) -> decltype(auto) { return (*pointer); }),
        "\n\n");

    return absl::StrCat("\n\n", ad_utility::addIndentation(list, "    "));
  };

  stream << getMetadataPrettyString(metadata(), "metadata: ", "\n");

  // Listing all the `ResultEntry`s, if there are any.
  stream << "Measurements:" << vectorToStringListOrNone(resultEntries_);

  // Listing all the `ResultTable`s, if there are any.
  stream << "\n\nTables:" << vectorToStringListOrNone(resultTables_);

  return absl::StrCat("Group '", descriptor_, "'\n",
                      ad_utility::addIndentation(stream.str(), "    "));
}

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const ResultGroup& resultGroup) {
  j = nlohmann::json{{"descriptor", resultGroup.descriptor_},
                     {"resultEntries", resultGroup.resultEntries_},
                     {"resultTables", resultGroup.resultTables_},
                     {"metadata", resultGroup.metadata()}};
}

// ____________________________________________________________________________
void ResultTable::init(const std::string& descriptor,
                       std::string descriptorForLog,
                       const std::vector<std::string>& rowNames,
                       const std::vector<std::string>& columnNames) {
  // Having a table without any columns makes no sense.
  if (columnNames.empty()) {
    throw ad_utility::Exception(
        absl::StrCat("A `ResultTable` must have at"
                     " least one column. Table '",
                     descriptor, "' has ", columnNames.size(), " columns"));
  }

  // Setting the member variables.
  descriptor_ = descriptor;
  descriptorForLog_ = std::move(descriptorForLog);
  columnNames_ = columnNames;
  entries_.resize(rowNames.size());
  std::ranges::fill(entries_, std::vector<EntryType>(columnNames.size()));

  // Setting the row names.
  for (size_t row = 0; row < rowNames.size(); row++) {
    setEntry(row, 0, rowNames.at(row));
  }
}

// ____________________________________________________________________________
ResultTable::ResultTable(const std::string& descriptor,
                         const std::vector<std::string>& rowNames,
                         const std::vector<std::string>& columnNames) {
  init(descriptor, descriptor, rowNames, columnNames);
}

// ____________________________________________________________________________
ResultTable::ResultTable(const std::string& descriptor,
                         std::string descriptorForLog,
                         const std::vector<std::string>& rowNames,
                         const std::vector<std::string>& columnNames) {
  init(descriptor, std::move(descriptorForLog), rowNames, columnNames);
}

// ____________________________________________________________________________
void ResultTable::setEntry(const size_t& row, const size_t& column,
                           const EntryType& newEntryContent) {
  // 'Deleting' an entry doesn't make much sense.
  AD_CONTRACT_CHECK(!std::holds_alternative<std::monostate>(newEntryContent));

  entries_.at(row).at(column) = newEntryContent;
}

// ____________________________________________________________________________
ResultTable::operator std::string() const {
  // Used for the formating of floats in the table. They will always be
  // formated as having 4 values after the decimal point.
  static constexpr absl::string_view floatFormatSpecifier = "%.4f";

  // What should be printed between columns. Used for nicer formating.
  const std::string columnSeperator = " | ";

  // Convert an `EntryType` of `ResultTable` to a screen friendly
  // format.
  auto entryToStringVisitor = []<typename T>(const T& entry) {
    /*
    `EntryType` has multiple distinct possible types, that all need different
    handeling. Fortunaly, we can decide the handeling at compile time and
    throw the others away, using `if constexpr(std::is_same<...,...>::value)`.
    */
    if constexpr (std::is_same_v<T, std::monostate>) {
      // No value, print it as NA.
      return "NA"s;
    } else if constexpr (std::is_same_v<T, float>) {
      // There is a value, format it as specified.
      return absl::StrFormat(floatFormatSpecifier, entry);
    } else if constexpr (std::is_same_v<T, size_t> || std::is_same_v<T, int>) {
      // There is already a `std` function for this.
      return std::to_string(entry);
    } else if constexpr (std::is_same_v<T, std::string>) {
      return entry;
    } else if constexpr (std::is_same_v<T, bool>) {
      // Simple conversion.
      return entry ? "true"s : "false"s;
    } else {
      // Unsupported type.
      AD_FAIL();
    }
  };
  auto entryToString = [&entryToStringVisitor](const EntryType& entry) {
    // The std::visit checks which type a std::variant is and calls our
    // visitor function with the right template parameter.
    return std::visit(entryToStringVisitor, entry);
  };

  /*
  Return a string with enough padding, empty spaces to the right of the string,
  to reach the wanted length. Doesn't shorten the given string.
  */
  auto addPaddingToString = [](std::string_view text,
                               const size_t wantedLength) {
    const std::string padding(
        wantedLength >= text.length() ? wantedLength - text.length() : 0, ' ');
    return absl::StrCat(text, padding);
  };

  /*
  @brief Creates the string representation of a row.

  @param rowEntries The entries for the row.
  @param rowEntryWidth The size, that the string representation of one entry in
  the row should take up. If the corresponding `rowEntries` string
  representation (same index in the vector) is to small, it will be padded out
  with empty spaces.
  */
  auto createRowString = [&columnSeperator, &addPaddingToString,
                          &entryToString](
                             const std::vector<EntryType>& rowEntries,
                             const std::vector<size_t>& rowEntryWidth) {
    AD_CONTRACT_CHECK(rowEntries.size() == rowEntryWidth.size());

    // The `rowEntryAsString` padded to the wanted width.
    /*
    TODO Can be replaced with `std::ranges::views::zip` and
    `std::ranges::view::transform`, once we have support for
    `std::ranges::views::zip`.
    */
    std::vector<std::string> rowEntryAsString;
    rowEntryAsString.reserve(rowEntries.size());
    for (size_t i = 0; i < rowEntries.size(); i++) {
      rowEntryAsString.push_back(addPaddingToString(
          entryToString(rowEntries.at(i)), rowEntryWidth.at(i)));
    }

    return ad_utility::lazyStrJoin(rowEntryAsString, columnSeperator);
  };

  // The prefix. Everything after this will be indented, so it's better
  // to only combine them at the end.
  std::string prefix = absl::StrCat("Table '", descriptor_, "'\n");

  // For printing the table.
  std::ostringstream stream;

  // Adding the metadata.
  stream << getMetadataPrettyString(metadata(), "metadata: ", "\n");

  // Transforming the column names into the table entry types, so that they can
  // share helper functions.
  auto columnNamesAsEntryType = ad_utility::transform(
      columnNames_,
      [](const std::string& name) { return static_cast<EntryType>(name); });

  // It's allowed to have tables without rows. In that case, we are already
  // nearly done, cause we only to have add the column names.
  if (numRows() == 0) {
    // Adding the column names. We don't need any padding.
    stream << createRowString(
        columnNamesAsEntryType,
        ad_utility::transform(columnNames_, &std::string::length));

    // Signal, that the table is empty.
    stream << "\n## Empty Table (0 rows) ##";

    return absl::StrCat(prefix,
                        ad_utility::addIndentation(stream.str(), "    "));
  }

  // For formating: What is the maximum string width of a column, if you
  // compare all it's entries?
  std::vector<size_t> columnMaxStringWidth(numColumns(), 0);
  for (size_t column = 0; column < numColumns(); column++) {
    // How long is the string representation of a colum entry in a wanted row?
    auto stringWidthOfRow = std::views::transform(
        entries_, [&column, &entryToString](const std::vector<EntryType>& row) {
          return entryToString(row.at(column)).length();
        });

    // Which of the entries is the longest?
    columnMaxStringWidth.at(column) = std::ranges::max(stringWidthOfRow);

    // Is the name of the column bigger?
    columnMaxStringWidth.at(column) = std::ranges::max(
        columnMaxStringWidth.at(column), columnNames_.at(column).length());
  }

  // Combining the column names and the table entries, so that printing is
  // easier.
  std::vector<const std::vector<EntryType>*> wholeTable;
  wholeTable.push_back(&columnNamesAsEntryType);
  ad_utility::appendVector(
      wholeTable,
      ad_utility::transform(entries_, [](const auto& row) { return &row; }));

  // Printing the table.
  ad_utility::lazyStrJoin(
      &stream,
      std::views::transform(
          wholeTable,
          [&createRowString, &columnMaxStringWidth](const auto& ptr) {
            return createRowString(*ptr, columnMaxStringWidth);
          }),
      "\n");

  return absl::StrCat(prefix, ad_utility::addIndentation(stream.str(), "    "));
}

// ____________________________________________________________________________
void ResultTable::addRow() {
  // Create an emptry row of the same size as every other row.
  entries_.emplace_back(numColumns());
}

// ____________________________________________________________________________
void ResultTable::deleteRow(const size_t rowIdx) {
  AD_CONTRACT_CHECK(rowIdx < numRows());
  entries_.erase(std::begin(entries_) + rowIdx);
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

// The code for the string insertion operator of a class, that can be casted to
// string.
static std::ostream& streamInserterOperatorImplementation(std::ostream& os,
                                                          const auto& obj) {
  os << static_cast<std::string>(obj);
  return os;
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const ResultEntry& resultEntry) {
  return streamInserterOperatorImplementation(os, resultEntry);
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const ResultTable& resultTable) {
  return streamInserterOperatorImplementation(os, resultTable);
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const ResultGroup& resultGroup) {
  return streamInserterOperatorImplementation(os, resultGroup);
}

// ____________________________________________________________________________
template <ad_utility::SameAsAny<ResultEntry, ResultTable> T>
void ResultGroup::deleteEntryImpl(T& entry) {
  // The vector, that holds our entries.
  auto& vec = [this]() -> auto& {
    if constexpr (std::same_as<T, ResultEntry>) {
      return resultEntries_;
    } else {
      static_assert(std::same_as<T, ResultTable>);
      return resultTables_;
    }
  }();

  // Delete `entry`.
  auto entryIterator{std::ranges::find(
      vec, &entry, [](const ad_utility::CopyableUniquePtr<T>& pointer) {
        return pointer.get();
      })};
  AD_CONTRACT_CHECK(entryIterator != std::end(vec));
  vec.erase(entryIterator);
}

// ____________________________________________________________________________
void ResultGroup::deleteMeasurement(ResultEntry& entry) {
  deleteEntryImpl(entry);
}

// ____________________________________________________________________________
void ResultGroup::deleteTable(ResultTable& table) { deleteEntryImpl(table); }

}  // namespace ad_benchmark
