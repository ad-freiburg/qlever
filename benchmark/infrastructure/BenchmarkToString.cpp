// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkToString.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include "BenchmarkMeasurementContainer.h"
#include "BenchmarkMetadata.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/StringUtils.h"

namespace ad_benchmark {

// ___________________________________________________________________________
void addCategoryTitleToOStringstream(std::ostringstream* stream,
                                     std::string_view categoryTitle) {
  // The bar above and below the title.
  const size_t barLength = categoryTitle.size() + 4;
  const std::string bar(barLength, '#');

  (*stream) << bar << "\n# " << categoryTitle << " #\n" << bar;
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

// ___________________________________________________________________________
void addVectorOfResultEntryToOStringstream(
    std::ostringstream* stream, const std::vector<ResultEntry>& entries,
    const std::string& vectorEntryPrefix, const std::string& newLinePrefix) {
  // What we use to seperat single vector entries.
  std::string_view lineSeparator{"\n\n"};

  // Adds a single `ResultEntry` to the stream.
  auto addResultEntry = [&stream, &vectorEntryPrefix,
                         &newLinePrefix](const ResultEntry& entry) {
    (*stream) << absl::StrCat(
        vectorEntryPrefix,
        absl::StrReplaceAll(static_cast<std::string>(entry),
                            {{"\n", absl::StrCat("\n", newLinePrefix)}}));
  };

  /*
  Adding the entries to the stream in such a way, that we don't have a line
  separator at the end of that list.
  */
  ad_utility::forEachExcludingTheLastOne(
      entries,
      [&addResultEntry, &stream, &lineSeparator](const ResultEntry& entry) {
        addResultEntry(entry);
        (*stream) << lineSeparator;
      },
      addResultEntry);
};

// ___________________________________________________________________________
void addSingleMeasurementsToOStringstream(
    std::ostringstream* stream, const std::vector<ResultEntry>& resultEntries) {
  addCategoryTitleToOStringstream(stream, "Single measurement benchmarks");
  (*stream) << "\n";
  addVectorOfResultEntryToOStringstream(stream, resultEntries, "", "");
}

// ___________________________________________________________________________
void addGroupsToOStringstream(std::ostringstream* stream,
                              const std::vector<ResultGroup>& resultGroups) {
  addCategoryTitleToOStringstream(stream, "Group benchmarks");
  (*stream) << "\n";
  addListToOStringStream(
      stream, resultGroups,
      [](const ResultGroup& group) { return static_cast<std::string>(group); },
      "\n\n");
}

// ___________________________________________________________________________
std::string vectorOfResultTableToListString(
    const std::vector<ResultTable>& tables) {
  std::ostringstream dummyStream;
  addListToOStringStream(
      &dummyStream, tables,
      [](const ResultTable& table) { return static_cast<std::string>(table); },
      "\n\n");
  return dummyStream.str();
}

// ___________________________________________________________________________
void addTablesToOStringstream(std::ostringstream* stream,
                              const std::vector<ResultTable>& resultTables) {
  addCategoryTitleToOStringstream(stream, "Table benchmarks");
  (*stream) << "\n" << vectorOfResultTableToListString(resultTables);
}

// ___________________________________________________________________________
static void addMetadataToOStringstream(std::ostringstream* stream,
                                       const BenchmarkMetadata& meta) {
  (*stream) << "General metadata: ";

  const std::string& metaString = getMetadataPrettyString(meta, "", "");

  // Just add none, if there isn't any.
  if (metaString == "") {
    (*stream) << "None";
  } else {
    (*stream) << metaString;
  }
}

// ___________________________________________________________________________
std::string ConfigOptions(const ad_utility::ConfigManager& config) {
  /*
  Because we want to create a list, we don't know how many entries there will be
  and need a string stream.
  */
  std::ostringstream stream;

  // Prints the default value of a configuration option and the accompanying
  // text.
  auto defaultConfigurationOptionToString =
      [](const ad_utility::ConfigOption& option) {
        return absl::StrCat("Configuration option '", option.getIdentifier(),
                            "' was not set at runtime, using default value '",
                            option.getDefaultValueAsString(), "'.");
      };

  ad_utility::forEachExcludingTheLastOne(
      config.getConfigurationOptions(),
      [&stream, &defaultConfigurationOptionToString](
          const ad_utility::ConfigOption& option) {
        if (option.hasDefaultValue() && !option.wasSetAtRuntime()) {
          stream << defaultConfigurationOptionToString(option) << "\n";
        }
      },
      [&stream, &defaultConfigurationOptionToString](
          const ad_utility::ConfigOption& option) {
        if (option.hasDefaultValue() && !option.wasSetAtRuntime()) {
          stream << defaultConfigurationOptionToString(option);
        }
      });

  return stream.str();
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

  // @brief Adds a category to the string steam, if it is not empty. Mainly
  //  exists for reducing code duplication.
  //
  // @param stringStream The stringstream where the text will get added.
  // @param categoryResult The information needed, for printing the benchmark
  //  category. Should be a vector of ResultEntry, ResultGroup, or ResultTable.
  // @param categoryAddPrintStreamFunction The function, which given the
  //  benchmark category information, converts them to text, and adds that text
  //  to the stringstream.
  auto addNonEmptyCategorieToStringSteam =
      [](std::ostringstream* stringStream, const auto& categoryResult,
         const auto& categoryAddPrintStreamFunction) {
        if (categoryResult.size() > 0) {
          // The separator between the printed categories.
          (*stringStream) << "\n\n";

          categoryAddPrintStreamFunction(stringStream, categoryResult);
        }
      };

  addCategoryTitleToOStringstream(
      &visualization,
      absl::StrCat("Benchmark class '", benchmarkClass->name(), "'"));
  visualization << "\n";

  // Visualize the general metadata.
  addMetadataToOStringstream(&visualization, benchmarkClass->getMetadata());

  // Visualization for single measurments, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, singleMeasurements,
                                    addSingleMeasurementsToOStringstream);

  // Visualization for groups, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, resultGroups,
                                    addGroupsToOStringstream);

  // Visualization for tables, if there are any.
  addNonEmptyCategorieToStringSteam(&visualization, resultTables,
                                    addTablesToOStringstream);

  return visualization.str();
}
}  // namespace ad_benchmark
