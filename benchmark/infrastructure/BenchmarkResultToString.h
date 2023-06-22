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

// How the indention should look like.
static constexpr std::string_view outputIndentation = "    ";

/*
@brief Applies the given function `regularFunction` to all elements in `r`,
except for the last one. Instead, `lastOneFunction` is applied to that one.

@tparam Range Needs to be a data type supported by `std::ranges`.

@param r Must hold at least one element.
*/
template <typename Range, typename RegularFunction, typename LastOneFunction>
static void forEachExcludingTheLastOne(const Range& r,
                                       RegularFunction regularFunction,
                                       LastOneFunction lastOneFunction) {
  // Throw an error, if there are no elements in `r`.
  AD_CONTRACT_CHECK(r.size() > 0);

  std::ranges::for_each_n(r.begin(), r.size() - 1, regularFunction, {});
  lastOneFunction(r.back());
}

/*
@brief Adds indention before the given string and directly after new line
characters.

@param indentionLevel How deep is the indention? `0` is no indention.
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
