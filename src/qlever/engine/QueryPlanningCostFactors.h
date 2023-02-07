// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <string>

#include "../util/HashMap.h"

using std::string;

// Simple container for cost factors.
// Comes with default values, that can be set and read from a file
class QueryPlanningCostFactors {
 public:
  QueryPlanningCostFactors();
  void readFromFile(const string& fileName);
  double getCostFactor(const string& key) const;

 private:
  ad_utility::HashMap<string, double> _factors;
};