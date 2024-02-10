// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)
#include "../benchmark/infrastructure/BenchmarkToJson.h"

#include <algorithm>
#include <concepts>
#include <iterator>
#include <utility>

#include "../benchmark/infrastructure/Benchmark.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_benchmark {
// ___________________________________________________________________________
void to_json(nlohmann::ordered_json& j, const BenchmarkResults& results) {
  // Creating the json object. We actually don't want `BenchmarkResults` to
  // be serialized, because that is the management class for measured
  // benchmarks. We just want the measured benchmarks.
  j = nlohmann::ordered_json{
      {"single-measurements", results.getSingleMeasurements()},
      {"result-groups", results.getGroups()},
      {"result-tables", results.getTables()}};
}

/*
@brief Transforms the content of a vector into a json array, using a
provided translation function for the vector entries.

@tparam TranslationFunction Has to be a function, that takes `VectorType`
and returns a `nlohmann:json` object.
*/
template <typename VectorType, ad_utility::InvocableWithExactReturnType<
                                   nlohmann::ordered_json, VectorType>
                                   TranslationFunction>
static nlohmann::json transformIntoJsonArray(
    const std::vector<VectorType>& vec,
    TranslationFunction translationFunction) {
  /*
  Without explicit instantiation, `nlohmann::nlohmann::ordered_json` is not
  guaranteed, to always interpret a `push_back` correctly. For instance,
  push_back(`nlohmann::ordered_json{{"entry", 0}}`) could end up creating an
  object, not a list entry. See the official `push_back` documentation of
  `nlohmann::ordered_json`, if you want a better example.
  */
  nlohmann::ordered_json jsonArray = nlohmann::ordered_json::array();

  std::ranges::transform(vec, std::back_inserter(jsonArray),
                         translationFunction);

  return jsonArray;
}

// ___________________________________________________________________________
nlohmann::ordered_json benchmarkResultsToJson(
    const std::vector<BenchmarkResults>& results) {
  return transformIntoJsonArray(results, [](const BenchmarkResults& result) {
    return nlohmann::ordered_json{result};
  });
}

// ___________________________________________________________________________
nlohmann::ordered_json zipBenchmarkClassAndBenchmarkResultsToJson(
    const std::vector<std::pair<const BenchmarkInterface*, BenchmarkResults>>&
        benchmarkClassAndBenchmarkResults) {
  return transformIntoJsonArray(
      benchmarkClassAndBenchmarkResults, [](const auto& pair) {
        return nlohmann::ordered_json{
            {"name", pair.first->name()},
            {"general-metadata", pair.first->getGeneralMetadata()},
            {"measurements", pair.second}};
      });
}

}  // namespace ad_benchmark
