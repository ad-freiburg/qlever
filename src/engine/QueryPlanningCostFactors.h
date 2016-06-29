// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <unordered_map>

using std::string;
using std::unordered_map;

// Simple container for cost factors.
// Comes with default values, that can be set and read from a file
class QueryPlanningCostFactors {
public:
  QueryPlanningCostFactors();
  void readFromFile(const string& fileName);
  float getCostFactor(const string& key) const;
private:
  unordered_map<string, float> _factors;
};