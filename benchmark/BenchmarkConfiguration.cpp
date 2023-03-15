// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "util/json.h"
#include "../benchmark/BenchmarkConfiguration.h"

// ____________________________________________________________________________
void BenchmarkConfiguration::parseJsonString(const std::string& jsonString){
  data_ = nlohmann::json::parse(jsonString);
}

// ____________________________________________________________________________
void BenchmarkConfiguration::parseShortHand(const std::string& shortHandString){
  // TODO Actually implement this.
  return;
}
