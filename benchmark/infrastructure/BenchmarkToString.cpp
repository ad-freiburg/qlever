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
                      ad_utility::lazyStrJoin(categoryEntries, "\n\n"));
}

// ___________________________________________________________________________
std::string benchmarkResultsToString(
    const BenchmarkInterface* const benchmarkClass,
    const BenchmarkResults& results) {
  // Visualizes the measured times.
  std::ostringstream visualization;

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

  /*
  @brief Adds a category to the string steam, if it is not empty. Mainly
   exists for reducing code duplication.

  @param categoryName The name of the category
  @param categoryResult The information needed, for printing the benchmark
  category. Should be a vector of ResultEntry, ResultGroup, or ResultTable.
  */
  auto addNonEmptyCategoryToStringSteam = [&visualization](
                                              std::string_view categoryName,
                                              const auto& categoryResult) {
    if (categoryResult.size() > 0) {
      // The separator between the printed categories.
      visualization << "\n\n" << categoryToString(categoryName, categoryResult);
    }
  };

  // Visualization for single measurments, if there are any.
  addNonEmptyCategoryToStringSteam("Single measurement benchmarks",
                                   results.getSingleMeasurements());

  // Visualization for groups, if there are any.
  addNonEmptyCategoryToStringSteam("Group benchmarks", results.getGroups());

  // Visualization for tables, if there are any.
  addNonEmptyCategoryToStringSteam("Table benchmarks", results.getTables());

  return visualization.str();
}
}  // namespace ad_benchmark
