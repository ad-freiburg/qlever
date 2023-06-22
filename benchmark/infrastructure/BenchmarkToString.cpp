// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkToString.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include <sstream>

#include "BenchmarkMeasurementContainer.h"
#include "BenchmarkMetadata.h"
#include "util/Exception.h"
#include "util/Forward.h"

namespace ad_benchmark {

/*
 * @brief Return a string of the form
 * "#################
 * # categoryTitle #
 * #################"
 */
static std::string createCategoryTitle(std::string_view categoryTitle) {
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
@brief Return a string containing a titel version of `categoryName` and a string
list representation of all the given category entries.
Note: This function converts `CategoryType` objects by trying to cast them as
`std::string`.
*/
template <typename CategoryType>
static std::string categoryToString(
    std::string_view categoryName,
    const std::vector<CategoryType>& categoryEntries) {
  return absl::StrCat(createCategoryTitle(categoryName), "\n",
                      ad_utility::listToString(
                          categoryEntries,
                          [](const CategoryType& entry) {
                            return static_cast<std::string>(entry);
                          },
                          "\n\n"));
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
  @param categoryName The name of the category
  @param categoryResult The information needed, for printing the benchmark
  category. Should be a vector of ResultEntry, ResultGroup, or ResultTable.
  @param translationFunction The function, which given the category name and the
  benchmark category information, converts them to text.
  */
  auto addNonEmptyCategorieToStringSteam =
      [](std::ostringstream* stringStream, std::string_view categoryName,
         const auto& categoryResult, const auto& translationFunction) {
        if (categoryResult.size() > 0) {
          // The separator between the printed categories.
          (*stringStream) << "\n\n"
                          << translationFunction(categoryName, categoryResult);
        }
      };

  visualization << createCategoryTitle(absl::StrCat(
                       "Benchmark class '", benchmarkClass->name(), "'"))
                << "\n";

  // Visualize the general metadata.
  if (const std::string& metadataString = getMetadataPrettyString(
          benchmarkClass->getMetadata(), "General metadata: ", "");
      !metadataString.empty()) {
    visualization << metadataString;
  } else {
    visualization << "General metadata: None";
  }

  // Visualization for single measurments, if there are any.
  addNonEmptyCategorieToStringSteam(
      &visualization, "Single measurement benchmarks", singleMeasurements,
      categoryToString<ResultEntry>);

  // Visualization for groups, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, "Group benchmarks",
                                    resultGroups,
                                    categoryToString<ResultGroup>);

  // Visualization for tables, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, "Table benchmarks",
                                    resultTables,
                                    categoryToString<ResultTable>);

  return visualization.str();
}
}  // namespace ad_benchmark
