// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <fstream>
#include "../util/StringUtils.h"
#include "../util/Exception.h"
#include "./QueryPlanningCostFactors.h"


// _____________________________________________________________________________
QueryPlanningCostFactors::QueryPlanningCostFactors() : _factors() {
  // Set default values
  _factors["FILTER_PUNISH"] = 10.0;
  _factors["NO_FILTER_PUNISH"] = 1.0;
  _factors["FILTER_SELECTIVITY"] = 0.5;
  _factors["HASH_MAP_OPERATION_COST"] = 50.0;
}

// _____________________________________________________________________________
void QueryPlanningCostFactors::readFromFile(const string& fileName) {
  std::ifstream in(fileName.c_str());
  string line;
  while (std::getline(in, line)) {
    auto v = ad_utility::split(line, '\t');
    AD_CHECK_EQ(v.size(), 2);
    _factors[v[0]] = atol(v[1].c_str());
  }
}

// _____________________________________________________________________________
float QueryPlanningCostFactors::getCostFactor(const string& key) const {
  return _factors.find(key)->second;
}



