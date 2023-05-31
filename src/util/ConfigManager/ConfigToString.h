// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <algorithm>
#include <sstream>
#include <string_view>
#include <vector>

#include "util/ConfigManager/ConfigManager.h"
#include "util/ConfigManager/ConfigOption.h"

namespace ad_utility {

/*
@brief Returns the content of a `BenchmarkConfigurationOption::ValueType` object
as a string representation.
*/
std::string benchmarkConfigurationOptionValueTypeToString(
    const ad_benchmark::BenchmarkConfigurationOption::ValueType& val);

/*
@brief Return a string, containing a list of all configuration options, that
weren't set at runtime, with their default values.
*/
std::string getDefaultValueBenchmarkConfigurationOptions(
    const ad_benchmark::BenchmarkConfiguration& config);
}  // namespace ad_utility
