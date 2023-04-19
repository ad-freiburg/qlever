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
#include "util/json.h"

namespace ad_benchmark {
/*
 * @brief Add a string of the form
 * "
 *  #################
 *  # categoryTitle #
 *  #################
 *
 *  "
 *  to the stream.
 */
void addCategoryTitleToOStringstream(std::ostringstream* stream,
                                     std::string_view categoryTitle);

// Default way of adding a vector of ResultEntrys to a `ostringstream` with
// optional prefix.
void addVectorOfResultEntryToOStringstream(
    std::ostringstream* stream, const std::vector<ResultEntry>& entries,
    const std::string& prefix = "");

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
 * @brief Returns a formated string containing all benchmark information.
 */
std::string benchmarkResultsToString(const BenchmarkResults& results);
}  // namespace ad_benchmark
