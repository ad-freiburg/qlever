// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

# pragma once

#include <vector>

#include "util/json.h"
#include "../benchmark/Benchmark.h"

/*
 * @brief Create a nlohmann::json object with all relevant informations
 *  about the measurments taken by a BenchmarkRecords.
 */
nlohmann::json benchmarksToJson(const BenchmarkRecords& records);
