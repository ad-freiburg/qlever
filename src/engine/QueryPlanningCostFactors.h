// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_QUERYPLANNINGCOSTFACTORS_H
#define QLEVER_SRC_ENGINE_QUERYPLANNINGCOSTFACTORS_H

#include <string>

#include "util/HashMap.h"

// Simple container for cost factors with fixed default values.
class QueryPlanningCostFactors {
 public:
  QueryPlanningCostFactors();
  double getCostFactor(const std::string& key) const;

 private:
  ad_utility::HashMap<std::string, double> _factors;
};

#endif  // QLEVER_SRC_ENGINE_QUERYPLANNINGCOSTFACTORS_H
