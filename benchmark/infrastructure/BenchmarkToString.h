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
#include "util/json.h"

namespace ad_benchmark {

// How the indentation should look like.
static constexpr std::string_view outputIndentation = "    ";

/*
@brief Adds indentation before the given string and directly after new line
characters.

@param indentationLevel How deep is the indentation? `0` is no indentation.
*/
std::string addIndentation(const std::string_view str,
                           const size_t& indentationLevel);

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
 * @brief Returns a formated string containing all the benchmark information.
 */
std::string benchmarkResultsToString(
    const BenchmarkInterface* const benchmarkClass,
    const BenchmarkResults& results);
}  // namespace ad_benchmark
