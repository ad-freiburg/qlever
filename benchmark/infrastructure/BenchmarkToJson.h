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
 * @brief Create a nlohmann::json array with all relevant informations
 *  about the measurments taken by all the `BenchmarkResults`.
 */
nlohmann::json benchmarkResultsToJson(
    const std::vector<BenchmarkResults>& results);

/*
@brief Create a nlohmann::json array with all relevant informations
given by the pairs. That is, all the `BenchmarkMetadata` and all information
defined by benchmarks, with every pair grouped up.
*/
nlohmann::json zipGeneralMetadataAndBenchmarkResultsToJson(
    const std::vector<std::pair<BenchmarkMetadata, BenchmarkResults>>&
        generalMetadataAndBenchmarkResults);
}  // namespace ad_benchmark
