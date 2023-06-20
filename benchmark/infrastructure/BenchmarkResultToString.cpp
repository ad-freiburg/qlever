// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include <sstream>

#include "../benchmark/infrastructure/BenchmarkResultToString.h"
#include "BenchmarkMeasurementContainer.h"
#include "BenchmarkMetadata.h"
#include "util/Exception.h"
#include "util/Forward.h"

namespace ad_benchmark {

// ___________________________________________________________________________
std::string createCategoryTitle(std::string_view categoryTitle) {
  // The bar above and below the title.
  const size_t barLength = categoryTitle.size() + 4;
  const std::string bar(barLength, '#');

  return absl::StrCat(bar, "\n# ", categoryTitle, " #\n", bar);
}

// ___________________________________________________________________________
std::string addIndentation(const std::string_view str,
                           const size_t& indentationLevel) {
  // An indention level of 0 makes no sense. Must be an error.
  AD_CONTRACT_CHECK(indentationLevel > 0);

  // The indention symbols for this level of indention.
  std::string indentationSymbols{""};
  indentationSymbols.reserve(outputIndentation.size() * indentationLevel);
  for (size_t i = 0; i < indentationLevel; i++) {
    indentationSymbols.append(outputIndentation);
  }

  // Add an indentation to the beginning and replace a new line with a new line,
  // directly followed by the indentation.
  return absl::StrCat(
      indentationSymbols,
      absl::StrReplaceAll(str,
                          {{"\n", absl::StrCat("\n", indentationSymbols)}}));
}

// ___________________________________________________________________________
std::string getMetadataPrettyString(const BenchmarkMetadata& meta,
                                    std::string_view prefix,
                                    std::string_view suffix) {
  const std::string& metadataString = meta.asJsonString(true);
  if (metadataString != "null") {
    return absl::StrCat(prefix, metadataString, suffix);
  } else {
    return "";
  }
}

/*
@brief Return elements of the range in form of a list.

@tparam TranslationFunction Must take the elements of the range and return a
string.

@param translationFunction Converts range elements into string.
@param listItemSeparator Will be put between each of the string representations
of the range elements.
*/
template <typename Range, typename TranslationFunction>
static std::string listToString(Range&& r,
                                TranslationFunction translationFunction,
                                std::string_view listItemSeparator = "\n") {
  std::ostringstream stream;

  /*
  TODO<C++23>: This can be a combination of `std::views::transform` and
  `std::views::join_with`. After that, we just have to insert all the elements
  of the new view into the stream.
  */
  forEachExcludingTheLastOne(
      AD_FWD(r),
      [&stream, &translationFunction,
       &listItemSeparator](const auto& listItem) {
        stream << translationFunction(listItem) << listItemSeparator;
      },
      [&stream, &translationFunction](const auto& listItem) {
        stream << translationFunction(listItem);
      });

  return stream.str();
}

// ___________________________________________________________________________
std::string vectorOfResultEntryToString(const std::vector<ResultEntry>& entries,
                                        const std::string& vectorEntryPrefix,
                                        const std::string& newLinePrefix) {
  std::ostringstream stream;

  // What we use to seperat single vector entries.
  std::string_view lineSeparator{"\n\n"};

  // Adds a single `ResultEntry` to the stream.
  auto addResultEntry = [&stream, &vectorEntryPrefix,
                         &newLinePrefix](const ResultEntry& entry) {
    stream << absl::StrCat(
        vectorEntryPrefix,
        absl::StrReplaceAll(static_cast<std::string>(entry),
                            {{"\n", absl::StrCat("\n", newLinePrefix)}}));
  };

  /*
  Adding the entries to the stream in such a way, that we don't have a line
  separator at the end of that list.
  */
  forEachExcludingTheLastOne(
      entries,
      [&addResultEntry, &stream, &lineSeparator](const ResultEntry& entry) {
        addResultEntry(entry);
        stream << lineSeparator;
      },
      addResultEntry);

  return stream.str();
};

// ___________________________________________________________________________
std::string singleMeasurementsToString(
    const std::vector<ResultEntry>& resultEntries) {
  return absl::StrCat(createCategoryTitle("Single measurement benchmarks"),
                      "\n", vectorOfResultEntryToString(resultEntries, "", ""));
}

// ___________________________________________________________________________
std::string groupsToString(const std::vector<ResultGroup>& resultGroups) {
  return absl::StrCat(createCategoryTitle("Group benchmarks"), "\n",
                      listToString(
                          resultGroups,
                          [](const ResultGroup& group) {
                            return static_cast<std::string>(group);
                          },
                          "\n\n"));
}

// ___________________________________________________________________________
std::string tablesToString(const std::vector<ResultTable>& resultTables) {
  return absl::StrCat(createCategoryTitle("Table benchmarks"), "\n",
                      listToString(
                          resultTables,
                          [](const ResultTable& table) {
                            return static_cast<std::string>(table);
                          },
                          "\n\n"));
}

// ___________________________________________________________________________
static std::string metadataToString(const BenchmarkMetadata& meta) {
  const std::string& metaString = getMetadataPrettyString(meta, "", "");

  // Just print none, if there isn't any.
  if (metaString.empty()) {
    return absl::StrCat("General metadata: None");
  } else {
    return absl::StrCat("General metadata: ", metaString);
  }
}

// ___________________________________________________________________________
std::string benchmarkResultsToString(
    const BenchmarkInterface* const benchmarkClass,
    const BenchmarkResults& results) {
  // The values for all the categories of benchmarks.
  const std::vector<ResultEntry>& singleMeasurements =
      results.getSingleMeasurements();
  const std::vector<ResultGroup>& resultGroups = results.getGroups();
  const std::vector<ResultTable>& resultTables = results.getTables();

  // Visualizes the measured times.
  std::ostringstream visualization;

  /*
  @brief Adds a category to the string steam, if it is not empty. Mainly
   exists for reducing code duplication.

  @param stringStream The stringstream where the text will get added.
  @param categoryResult The information needed, for printing the benchmark
  category. Should be a vector of ResultEntry, ResultGroup, or ResultTable.
  @param translationFunction The function, which given the benchmark category
  information, converts them to text.
  */
  auto addNonEmptyCategorieToStringSteam = [](std::ostringstream* stringStream,
                                              const auto& categoryResult,
                                              const auto& translationFunction) {
    if (categoryResult.size() > 0) {
      // The separator between the printed categories.
      (*stringStream) << "\n\n" << translationFunction(categoryResult);
    }
  };

  visualization << createCategoryTitle(absl::StrCat(
                       "Benchmark class '", benchmarkClass->name(), "'"))
                << "\n";

  // Visualize the general metadata.
  visualization << metadataToString(benchmarkClass->getMetadata());

  // Visualization for single measurments, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, singleMeasurements,
                                    singleMeasurementsToString);

  // Visualization for groups, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, resultGroups,
                                    groupsToString);

  // Visualization for tables, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, resultTables,
                                    tablesToString);

  return visualization.str();
}
}  // namespace ad_benchmark
