// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./QueryPlanningCostFactors.h"

#include <absl/strings/charconv.h>
#include <absl/strings/str_split.h>

#include <fstream>

#include "util/Exception.h"
#include "util/Log.h"
#include "util/StringUtils.h"

// _____________________________________________________________________________
QueryPlanningCostFactors::QueryPlanningCostFactors() : _factors() {
  // Set default values
  _factors["FILTER_PUNISH"] = 2.0;
  _factors["NO_FILTER_PUNISH"] = 1.0;
  _factors["FILTER_SELECTIVITY"] = 0.1;
  _factors["HASH_MAP_OPERATION_COST"] = 50.0;
  _factors["JOIN_SIZE_ESTIMATE_CORRECTION_FACTOR"] = 0.7;
  _factors["DUMMY_JOIN_SIZE_ESTIMATE_CORRECTION_FACTOR"] = 0.7;

  // Assume that a random disk seek is 100 times more expensive than an
  // average `O(1)` access to a single ID.
  _factors["DISK_RANDOM_ACCESS_COST"] = 100;
}

// _____________________________________________________________________________

float toFloat(std::string_view view) {
  float factor;
  auto last = view.data() + view.size();
  auto [ptr, ec] = absl::from_chars(view.data(), last, factor);
  if (ec != std::errc() || ptr != last) {
    throw std::runtime_error{std::string{"Invalid float: "} + view};
  }
  return factor;
}

// _____________________________________________________________________________
void QueryPlanningCostFactors::readFromFile(const string& fileName) {
  std::ifstream in(fileName);
  string line;
  while (std::getline(in, line)) {
    std::vector<std::string_view> v = absl::StrSplit(line, '\t');
    AD_CONTRACT_CHECK(v.size() == 2);
    float factor = toFloat(v[1]);
    LOG(INFO) << "Setting cost factor: " << v[0] << " from " << _factors[v[0]]
              << " to " << factor << std::endl;
    _factors[v[0]] = factor;
  }
}

// _____________________________________________________________________________
double QueryPlanningCostFactors::getCostFactor(const string& key) const {
  return _factors.find(key)->second;
}
