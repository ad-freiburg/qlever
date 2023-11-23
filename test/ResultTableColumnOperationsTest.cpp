// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/util/ResultTableColumnOperations.h"
#include "util/BenchmarkMeasurementContainerHelpers.h"

namespace ad_benchmark {

TEST(ResultTableColumnOperations, generateColumnWithColumnInput) {
  // Single parameter operators.
  doForTypeInResultTableEntryType([]<typename T>() {
    // How many rows should the test table have?
  });
}
}  // namespace ad_benchmark
