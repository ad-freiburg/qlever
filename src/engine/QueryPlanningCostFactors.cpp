// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./QueryPlanningCostFactors.h"

#include <fstream>

#include "../util/Exception.h"
#include "../util/Log.h"
#include "../util/StringUtils.h"

// _____________________________________________________________________________
QueryPlanningCostFactors::QueryPlanningCostFactors() : _factors() {
  // Set default values
  _factors["FILTER_PUNISH"] = 2.0;
  _factors["NO_FILTER_PUNISH"] = 1.0;
  _factors["FILTER_SELECTIVITY"] = 0.1;
  _factors["HASH_MAP_OPERATION_COST"] = 50.0;
  _factors["JOIN_SIZE_ESTIMATE_CORRECTION_FACTOR"] = 0.7;
  _factors["DUMMY_JOIN_SIZE_ESTIMATE_CORRECTION_FACTOR"] = 1000.0;
  _factors["DISK_RANDOM_ACCESS_COST"] = 1000;
}

// _____________________________________________________________________________
void QueryPlanningCostFactors::readFromFile(const string& fileName) {
  std::ifstream in(fileName.c_str());
  string line;
  while (std::getline(in, line)) {
    auto v = ad_utility::split(line, '\t');
    AD_CHECK_EQ(v.size(), 2);
    LOG(INFO) << "Setting cost factor: " << v[0] << " from " << _factors[v[0]]
              << " to " << atof(v[1].c_str()) << '\n';
    _factors[v[0]] = atof(v[1].c_str());
  }
}

// _____________________________________________________________________________
double QueryPlanningCostFactors::getCostFactor(const string& key) const {
  return _factors.find(key)->second;
}
