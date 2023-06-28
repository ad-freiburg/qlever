// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <algorithm>
#include <sstream>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "BenchmarkMetadata.h"
#include "util/Algorithm.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/json.h"

namespace ad_benchmark {

/*
 * @brief Add a string of the form
 * "#################
 * # categoryTitle #
 * #################"
 *  to the stream.
 */
void addCategoryTitleToOStringstream(std::ostringstream* stream,
                                     std::string_view categoryTitle);

/*
@brief Adds the elements of the given range to the stream in form of a list.

@tparam TranslationFunction Must take the elements of the range and return a
string.

@param translationFunction Converts range elements into string.
@param listItemSeparator Will be put between each of the string representations
of the range elements.
*/
template <typename Range, typename TranslationFunction>
static void addListToOStringStream(std::ostringstream* stream, Range&& r,
                                   TranslationFunction translationFunction,
                                   std::string_view listItemSeparator = "\n") {
  /*
  TODO<C++23>: This can be a combination of `std::views::transform` and
  `std::views::join_with`. After that, we just have to insert all the elements
  of the new view into the stream.
  */
  ad_utility::forEachExcludingTheLastOne(
      AD_FWD(r),
      [&stream, &translationFunction,
       &listItemSeparator](const auto& listItem) {
        (*stream) << translationFunction(listItem) << listItemSeparator;
      },
      [&stream, &translationFunction](const auto& listItem) {
        (*stream) << translationFunction(listItem);
      });
}

/*
@brief If `meta` is a non empty metadata object, return it's non compact
json string representation. Otherwise, return an empty string.

@param prefix Will be printed before the json string.
@param suffix Will be printed after the json string.
*/
std::string getMetadataPrettyString(const BenchmarkMetadata& meta,
                                    std::string_view prefix,
                                    std::string_view suffix);

/*
@brief Add a vector of `ResultEntry` in their string form to the string stream
in form of a list.

@param vectorEntryPrefix A prefix added before every entry in the vector.
@param newLinePrefix A prefix added before the start of a new line.
*/
// Default way of adding a vector of ResultEntrys to a `ostringstream` with
// optional prefix, which will be inserted at the start of every new line.
void addVectorOfResultEntryToOStringstream(
    std::ostringstream* stream, const std::vector<ResultEntry>& entries,
    const std::string& vectorEntryPrefix, const std::string& newLinePrefix);

/*
@brief Return a vector of `ResultTable` in their string form in form of a list.
*/
std::string vectorOfResultTableToListString(
    const std::vector<ResultTable>& tables);

// Visualization for single measurments.
void addSingleMeasurementsToOStringstream(
    std::ostringstream* stream, const std::vector<ResultEntry>& resultEntries);

// Visualization for groups.
void addGroupsToOStringstream(std::ostringstream* stream,
                              const std::vector<ResultGroup>& resultGroups);

// Visualization for tables.
void addTablesToOStringstream(std::ostringstream* stream,
                              const std::vector<ResultTable>& resultTables);

/*
 * @brief Returns a formated string containing all the benchmark information.
 */
std::string benchmarkResultsToString(
    const BenchmarkInterface* const benchmarkClass,
    const BenchmarkResults& results);
}  // namespace ad_benchmark
