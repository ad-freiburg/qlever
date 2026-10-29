// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "engine/QueryPlanningCostFactors.h"

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
double QueryPlanningCostFactors::getCostFactor(const std::string& key) const {
  return _factors.find(key)->second;
}
