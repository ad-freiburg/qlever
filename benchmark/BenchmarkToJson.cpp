// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)
#include <algorithm>
#include <iterator>

#include "../benchmark/BenchmarkToJson.h"
#include "Benchmark.h"

// ___________________________________________________________________________
nlohmann::json benchmarksToJson(const BenchmarkRecords& records){
  // Creating the json object. We actually don't want BenchmarkRecords to
  // be serilized, because that is the management class for measured
  // benchmarks. We just want the measured benchmarks.
  return nlohmann::json{{"singleMeasurements", records.getSingleMeasurements()},
    {"recordGroups", records.getGroups()},
    {"recordTables", records.getTables()}};
}

// ___________________________________________________________________________
nlohmann::json benchmarksToJson(const std::vector<BenchmarkRecords>& records){
  nlohmann::json toReturn = nlohmann::json::array();
  std::ranges::transform(records, std::back_inserter(toReturn),
  [](const BenchmarkRecords& record){
      return benchmarksToJson(record);
    });
  return toReturn;
}