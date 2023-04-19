// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include "util/json.h"

namespace ad_benchmark {
/*
 * A rather basic wrapper for nlohmann::json, which only allows basic adding
 * of key value pairs and returning of the json string.
 */
class BenchmarkMetadata {
  // No real reason, to really build everything ourselves, when the
  // nlohmann::json object already containes everything, that we could need.
  nlohmann::json data_;

 public:
  /*
   * @brief Adds a key value pair to the metadata.
   *
   * @tparam T Can be anything, as long as the type is supported by
   *  `nlohmann::json`. If it isn't, you can look up, how to add
   *  types.
   *
   * @param key The name of the metadata entry.
   * @param value The content of the metadata entry.
   */
  template <typename T>
  void addKeyValuePair(const std::string& key, const T& value) {
    data_[key] = value;
  }

  /*
   * @brief Returns the metadata in form of a json string.
   */
  std::string asJsonString() const { return data_.dump(); }

  // JSON serialization.
  friend void to_json(nlohmann::json& j, const BenchmarkMetadata& metadata) {
    j = metadata.data_;
  }
};
}  // namespace ad_benchmark
