// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/BenchmarkRecordToJson.h"

// ___________________________________________________________________________
nlohmann::json benchmarksToJson(const BenchmarkRecords& records){
  // The values for all the categories of benchmarks.
  const std::vector<BenchmarkRecords::RecordEntry>& singleMeasurements =
    records.getSingleMeasurements();
  const std::vector<BenchmarkRecords::RecordGroup>& recordGroups =
    records.getGroups();
  const std::vector<BenchmarkRecords::RecordTable>& recordTables =
    records.getTables();

  // Creating the json object. We actually don't want BenchmarkRecords to
  // be serilized, because that is the management class for measured
  // benchmarks. We just want the measured benchmarks.
  return nlohmann::json{{"singleMeasurements", singleMeasurements},
    {"recordGroups", recordGroups}, {"recordTables", recordTables}};
}
