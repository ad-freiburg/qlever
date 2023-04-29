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
#include "../benchmark/infrastructure/BenchmarkResultToString.h"
#include "BenchmarkMetadata.h"
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
@brief If `meta` is a non empty metadata object, return it's non compact
json string representation. Otherwise, return an empty string.

@param prefix Will be printed before the json string.
@param suffix Will be printed after the json string.
*/
std::string getMetadataPrettyString(const BenchmarkMetadata& meta,
  std::string_view prefix, std::string_view suffix);

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
 * @brief Returns a formated string containing the given metadata, followed
 * by all the benchmark information.
 */
std::string benchmarkResultsToString(const BenchmarkMetadata& meta,
                                     const BenchmarkResults& results);
}  // namespace ad_benchmark
