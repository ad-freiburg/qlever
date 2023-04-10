// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <vector>
#include <utility>

#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "util/json.h"
#include "../benchmark/infrastructure/Benchmark.h"

/*
 * @brief Create a nlohmann::json array with all relevant informations
 *  about the measurments taken by all the `BenchmarkResults`.
 */
nlohmann::json benchmarkRecordsToJson(const std::vector<BenchmarkResults>& records);

/*
@brief Create a nlohmann::json array with all relevant informations
given by the pairs. That is, all the `BenchmarkMetadata` and all information
defined by benchmarks, with every pair grouped up.
*/
nlohmann::json zipGeneralMetadataAndBenchmarkResultsToJson(
 const std::vector<std::pair<BenchmarkMetadata, BenchmarkResults>>&
 generalMetadataAndBenchmarkResults);

/*
@brief Create a nlohmann::json array with all relevant informations
given by the two vectors. That is, all the `BenchmarkMetadata` and all
information defined by benchmarks, with every entry in a vector paired up
with the entry at the same place in the other vector.
*/
nlohmann::json zipGeneralMetadataAndBenchmarkResultsToJson(
 const std::vector<BenchmarkMetadata>& generalMetadata,
 const std::vector<BenchmarkResults>& benchmarkRecords);
