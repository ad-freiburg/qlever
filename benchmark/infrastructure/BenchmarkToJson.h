// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <utility>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "util/json.h"

namespace ad_benchmark {
/*
 * @brief Create a nlohmann::ordered_json array with all relevant information
 *  about the measurements taken by all the `BenchmarkResults`.
 */
nlohmann::ordered_json benchmarkResultsToJson(
    const std::vector<BenchmarkResults>& results);

/*
@brief Create a nlohmann::ordered_json array with all relevant information
given by the pairs.
*/
nlohmann::ordered_json zipBenchmarkClassAndBenchmarkResultsToJson(
    const std::vector<std::pair<const BenchmarkInterface*, BenchmarkResults>>&
        benchmarkClassAndBenchmarkResults);
}  // namespace ad_benchmark
