// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)
#include "../benchmark/infrastructure/BenchmarkToJson.h"

#include <algorithm>
#include <concepts>
#include <iterator>
#include <utility>

#include "../benchmark/infrastructure/Benchmark.h"
#include "nlohmann/json.hpp"

namespace ad_benchmark {
// ___________________________________________________________________________
void to_json(nlohmann::json& j, const BenchmarkResults& results) {
  // Creating the json object. We actually don't want `BenchmarkResults` to
  // be serialized, because that is the management class for measured
  // benchmarks. We just want the measured benchmarks.
  j = nlohmann::json{{"singleMeasurements", results.getSingleMeasurements()},
                     {"resultGroups", results.getGroups()},
                     {"resultTables", results.getTables()}};
}

/*
Requires `TranslationFunction` to be a invocable function, that takes one
parameter of a specific type and returns a `nlohmann::json` object.
*/
template <typename SourceType, typename TranslationFunction>
concept TransformsSourceTypeToJson =
    requires(TranslationFunction t, SourceType v) {
      { t(v) } -> std::same_as<nlohmann::json>;
    };

/*
@brief Transforms the content of a vector into a json array, using a
provided translation function for the vector entries.

@tparam TranslationFunction Has to be a function, that takes `VectorType`
and returns a `nlohmann:json` object.
*/
template <typename VectorType, typename TranslationFunction>
requires TransformsSourceTypeToJson<VectorType, TranslationFunction>
static nlohmann::json transformIntoJsonArray(
    const std::vector<VectorType>& vec,
    TranslationFunction translationFunction) {
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
nlohmann::json benchmarkResultsToJson(
    const std::vector<BenchmarkResults>& results) {
  return transformIntoJsonArray(results, [](const BenchmarkResults& result) {
    return nlohmann::json{result};
  });
}

// ___________________________________________________________________________
nlohmann::json zipBenchmarkClassAndBenchmarkResultsToJson(
    const std::vector<std::pair<const BenchmarkInterface*, BenchmarkResults>>&
        benchmarkClassAndBenchmarkResults) {
  return transformIntoJsonArray(
      benchmarkClassAndBenchmarkResults, [](const auto& pair) {
        return nlohmann::json{{"name", pair.first->name()},
                              {"general metadata", pair.first->getMetadata()},
                              {"measurements", pair.second}};
      });
}

}  // namespace ad_benchmark
