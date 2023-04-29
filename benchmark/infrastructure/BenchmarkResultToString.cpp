// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkResultToString.h"
#include "BenchmarkMeasurementContainer.h"
#include "BenchmarkMetadata.h"
#include "util/Exception.h"
#include "util/Forward.h"

#include <absl/strings/str_cat.h>
#include <bits/ranges_algo.h>

namespace ad_benchmark {

extern const std::string outputIndention = "\t";

// ___________________________________________________________________________
void addCategoryTitleToOStringstream(std::ostringstream* stream,
                                     std::string_view categoryTitle) {
  // The bar above and below the title.
  const size_t barLength = categoryTitle.size() + 4;
  const std::string bar(barLength, '#');

  (*stream) << bar << "\n# " << categoryTitle << " #\n" << bar;
}

// ___________________________________________________________________________
std::string addIndtention(const std::string_view str,
  const size_t& indentionLevel){
  // An indention level of 0 makes no sense. Must be an error.
  AD_CONTRACT_CHECK(indentionLevel > 0);

  // The indention symbols for this level of indention.
  std::string indentionSymbols{""};
  indentionSymbols.reserve(outputIndention.size() * indentionLevel);
  for (size_t i = 0; i < indentionLevel; i++){
      indentionSymbols.append(outputIndention);
    }

  std::ostringstream stream;
  stream << indentionSymbols;

  // Adding the content of the string char per char, so that we can easily
  // identify the new line symbols.
  std::ranges::for_each(str,
    [&indentionSymbols, &stream](const char& symbol){
      stream << symbol;
      if (symbol == '\n'){
        stream << indentionSymbols;
      }
    }, {});

  return stream.str();
}

// ___________________________________________________________________________
std::string getMetadataPrettyString(const BenchmarkMetadata& meta,
  std::string_view prefix, std::string_view suffix){
  const std::string& metadataString = meta.asJsonString(true);
  if (metadataString != "null"){
    return absl::StrCat(prefix, metadataString, suffix);
  } else {
    return "";
  }
}

/*
@brief Applies the given function `regularFunction` to all elements in `r`,
except for the last one. Instead, `lastOneFunction` is applied to that one.
*/
template<typename Range, typename RegularFunction, typename LastOneFunction>
static void forEachExcludingTheLastOne(Range&& r,
  RegularFunction regularFunction, LastOneFunction lastOneFunction){
  std::ranges::for_each_n(r.begin(), r.size() - 1, regularFunction, {});
  lastOneFunction(r.back());
}

/*
@brief Adds the elements of the given range to the stream in form of a list.

@tparam TranslationFunction Must take the elements of the range and return a
string.

@param translationFunction Converts range elements into string.
@param listItemSeperator Will be put between each of the string representations
of the range elements.
*/
template<typename Range, typename TranslationFunction>
static void addListToOStringStream(std::ostringstream* stream, Range&& r,
  TranslationFunction translationFunction,
  std::string_view listItemSeperator = "\n"){
  forEachExcludingTheLastOne(AD_FWD(r),
    [&stream, &translationFunction, &listItemSeperator](const auto& listItem){
      (*stream) << translationFunction(listItem) << listItemSeperator;
    },
    [&stream, &translationFunction](const auto& listItem){
      (*stream) << translationFunction(listItem);
    });
}

// ___________________________________________________________________________
void addVectorOfResultEntryToOStringstream(
    std::ostringstream* stream, const std::vector<ResultEntry>& entries,
    const std::string& vectorEntryPrefix, const std::string& newLinePrefix) {
  // What we use to seperat single vector entries.
  std::string_view lineSeperator{"\n\n"};

  // Adds a single `ResultEntry` to the stream.
  auto addResultEntry = [&stream, &vectorEntryPrefix,
                         &newLinePrefix](const ResultEntry& entry){
      (*stream) << vectorEntryPrefix;

      /*
      In order to effectivly add the prefix at the start of every new line, we
      feed the content of the string into the stream char for char.
      */
      std::ranges::for_each(static_cast<std::string>(entry),
                            [&stream,
                             &newLinePrefix](const char& currentSymbol) {
                              if (currentSymbol == '\n') {
                                (*stream) << "\n" << newLinePrefix;
                              } else {
                                (*stream) << currentSymbol;
                              }
                            },
                            {});
    };

  /*
  Adding the entries to the stream in such a way, that we don't have a line
  seperator at the end of that list.
  */
  forEachExcludingTheLastOne(entries,
    [&addResultEntry, &stream, &lineSeperator](const ResultEntry& entry){
      addResultEntry(entry);
      (*stream) << lineSeperator;
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
  addListToOStringStream(stream, resultGroups,
    [](const ResultGroup& group){return static_cast<std::string>(group);},
    "\n\n");
}

// ___________________________________________________________________________
void addTablesToOStringstream(std::ostringstream* stream,
                              const std::vector<ResultTable>& resultTables) {
  addCategoryTitleToOStringstream(stream, "Table benchmarks");
  (*stream) << "\n";
  addListToOStringStream(stream, resultTables,
    [](const ResultTable& table){return static_cast<std::string>(table);},
    "\n\n");
}

// ___________________________________________________________________________
static void addMetadataToOStringstream(std::ostringstream* stream,
  const BenchmarkMetadata& meta){
  (*stream) << "General metadata: ";

  const std::string& metaString = getMetadataPrettyString(meta, "", "");

  // Just add none, if there isn't any.
  if (metaString == ""){
    (*stream) << "None";
  } else {
    (*stream) << metaString;
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

  // The seperator between the printed categories.
  constexpr std::string_view seperator = "\n\n";

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
      [&seperator](std::ostringstream* stringStream, const auto& categoryResult,
         const auto& categoryAddPrintStreamFunction) {
        if (categoryResult.size() > 0) {
          (*stringStream) << seperator;
          categoryAddPrintStreamFunction(stringStream, categoryResult);
        }
      };

  addCategoryTitleToOStringstream(&visualization,
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
