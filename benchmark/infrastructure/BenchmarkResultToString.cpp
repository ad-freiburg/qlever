// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkResultToString.h"

#include <bits/ranges_algo.h>

namespace ad_benchmark {
// ___________________________________________________________________________
void addCategoryTitleToOStringstream(std::ostringstream* stream,
                                     std::string_view categoryTitle) {
  // The bar above and below the title.
  const size_t barLength = categoryTitle.size() + 4;
  const std::string bar(barLength, '#');

  (*stream) << "\n" << bar << "\n# " << categoryTitle << " #\n" << bar << "\n";
}

// ___________________________________________________________________________
void addVectorOfResultEntryToOStringstream(
    std::ostringstream* stream, const std::vector<ResultEntry>& entries,
    const std::string& vectorEntryPrefix, const std::string& newLinePrefix) {
  for (const auto& entry : entries) {
    // The beginning of the first row.
    (*stream) << "\n\n" << vectorEntryPrefix;

    /*
    In order to effectivly add the prefix at the start of every new line, we
    feed the content of the string into the stream char for char.
    */
    std::ranges::for_each(static_cast<std::string>(entry),
                          [&stream, &newLinePrefix](const char& currentSymbol) {
                            if (currentSymbol == '\n') {
                              (*stream) << "\n" << newLinePrefix;
                            } else {
                              (*stream) << currentSymbol;
                            }
                          },
                          {});
  }
};

// ___________________________________________________________________________
void addSingleMeasurementsToOStringstream(
    std::ostringstream* stream, const std::vector<ResultEntry>& resultEntries) {
  addCategoryTitleToOStringstream(stream, "Single measurment benchmarks");
  addVectorOfResultEntryToOStringstream(stream, resultEntries,
                                        "Single measurment benchmark ", "");
}

// ___________________________________________________________________________
void addGroupsToOStringstream(std::ostringstream* stream,
                              const std::vector<ResultGroup>& resultGroups) {
  addCategoryTitleToOStringstream(stream, "Group benchmarks");
  for (const auto& group : resultGroups) {
    (*stream) << "\n\n" << (std::string)group;
  }
}

// ___________________________________________________________________________
void addTablesToOStringstream(std::ostringstream* stream,
                              const std::vector<ResultTable>& resultTables) {
  addCategoryTitleToOStringstream(stream, "Table benchmarks");
  // Printing the tables themselves.
  for (const auto& table : resultTables) {
    (*stream) << "\n\n" << (std::string)table;
  }
}

// ___________________________________________________________________________
std::string benchmarkResultsToString(const BenchmarkMetadata& meta,
                                     const BenchmarkResults& results) {
  // The values for all the categories of benchmarks.
  const std::vector<ResultEntry>& singleMeasurements =
      results.getSingleMeasurements();
  const std::vector<ResultGroup>& resultGroups = results.getGroups();
  const std::vector<ResultTable>& resultTables = results.getTables();

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
  auto addNonEmptyCategorieToStringSteam =
      [](std::ostringstream* stringStream, const auto& categoryResult,
         const auto& categoryAddPrintStreamFunction,
         const std::string& suffix = "") {
        if (categoryResult.size() > 0) {
          categoryAddPrintStreamFunction(stringStream, categoryResult);
          (*stringStream) << suffix;
        }
      };

  // Visualize the general metadata.
  addCategoryTitleToOStringstream(&visualization,
                                  "General metadata for the"
                                  " following benchmarks");
  visualization << "\n\n" << meta.asJsonString(true) << "\n\n";

  // Visualization for single measurments, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, singleMeasurements,
                                    addSingleMeasurementsToOStringstream,
                                    "\n\n");

  // Visualization for groups, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, resultGroups,
                                    addGroupsToOStringstream, "\n\n");

  // Visualization for tables, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, resultTables,
                                    addTablesToOStringstream);

  return visualization.str();
}
}  // namespace ad_benchmark
