// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)
#include <algorithm>
#include <iterator>
#include <concepts>
#include <utility>

#include "../benchmark/BenchmarkToJson.h"
#include "Benchmark.h"
#include "nlohmann/json.hpp"

// ___________________________________________________________________________
nlohmann::json benchmarkRecordsToJson(const BenchmarkRecords& records){
  // Creating the json object. We actually don't want BenchmarkRecords to
  // be serilized, because that is the management class for measured
  // benchmarks. We just want the measured benchmarks.
  return nlohmann::json{{"singleMeasurements", records.getSingleMeasurements()},
    {"recordGroups", records.getGroups()},
    {"recordTables", records.getTables()}};
}

/*
@brief Transforms the content of a vector into a json array, using a
provided translation function for the vector entries.

@tparam TranslationFunction Has to be a function, that takes `VectorType`
and returns a `nlohmann:json` object.
*/
template<typename VectorType, typename TranslationFunction>
requires requires(TranslationFunction t, VectorType v){
  {t(v)} -> std::same_as<nlohmann::json>;
}
static nlohmann::json transformIntoJsonArray(const std::vector<VectorType>& vec,
  TranslationFunction translationFunction){
  // Without explicit instantiation, `nlohmann::json` is not guaranteed, to
  // always interpret a `push_back` correctly. For instance,
  // push_back(`nlohmann::json{{"entry", 0}}`) could end up creating an object,
  // not a list entry.
  // See the official `push_back` documentation of `nlohmann::json`, if you
  // want a better example.
  nlohmann::json jsonArray = nlohmann::json::array();

  std::ranges::transform(vec, std::back_inserter(jsonArray),
    translationFunction);

  return jsonArray;
}

// ___________________________________________________________________________
nlohmann::json benchmarkRecordsToJson(const std::vector<BenchmarkRecords>& records){
  return transformIntoJsonArray(records, [](const BenchmarkRecords& record){
    return benchmarkRecordsToJson(record);
  });
}


// ___________________________________________________________________________
nlohmann::json zipGeneralMetadataAndBenchmarkRecordsToJson(
 const std::vector<std::pair<BenchmarkMetadata, BenchmarkRecords>>&
 generalMetadataAndBenchmarkRecords){
  return transformIntoJsonArray(generalMetadataAndBenchmarkRecords,
  [](const auto& pair){
    return nlohmann::json{{"general metadata", pair.first},
    {"measurements", benchmarkRecordsToJson(pair.second)}};
  });
}

// ___________________________________________________________________________
nlohmann::json zipGeneralMetadataAndBenchmarkRecordsToJson(
 const std::vector<BenchmarkMetadata>& generalMetadata,
 const std::vector<BenchmarkRecords>& benchmarkRecords){
  // We can just pass this, after transforming it.
  std::vector<std::pair<BenchmarkMetadata, BenchmarkRecords>>
  vectorsPairedUp{};
  vectorsPairedUp.reserve(generalMetadata.size());

  std::ranges::transform(generalMetadata, benchmarkRecords,
    std::back_inserter(vectorsPairedUp),
    [](auto& a, auto& b){
      return std::make_pair(a, b);
    }
    );

  return zipGeneralMetadataAndBenchmarkRecordsToJson(vectorsPairedUp);
}