// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <vector>
#include <utility>

#include "BenchmarkMetadata.h"
#include "util/json.h"
#include "../benchmark/Benchmark.h"

/*
 * @brief Create a nlohmann::json object with all relevant informations
 *  about the measurments taken by a BenchmarkRecords.
 */
nlohmann::json benchmarkRecordsToJson(const BenchmarkRecords& records);

/*
 * @brief Create a nlohmann::json array with all relevant informations
 *  about the measurments taken by all the BenchmarkRecords.
 */
nlohmann::json benchmarkRecordsToJson(const std::vector<BenchmarkRecords>& records);

/*
@brief Create a nlohmann::json array with all relevant informations
given by the pairs. That is, all the `BenchmarkMetadata` and all information
defined by benchmarks, with every pair grouped up.
*/
nlohmann::json zipGeneralMetadataAndBenchmarkRecordsToJson(
 const std::vector<std::pair<BenchmarkMetadata, BenchmarkRecords>>&
 generalMetadataAndBenchmarkRecords);

/*
@brief Create a nlohmann::json array with all relevant informations
given by the two vectors. That is, all the `BenchmarkMetadata` and all
information defined by benchmarks, with every entry in a vector paired up
with the entry at the same place in the other vector.
*/
nlohmann::json zipGeneralMetadataAndBenchmarkRecordsToJson(
 const std::vector<BenchmarkMetadata>& generalMetadata,
 const std::vector<BenchmarkRecords>& benchmarkRecords);
