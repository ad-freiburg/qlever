// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include "util/json.h"

/*
 * A rather basic wrapper for nlohmann::json, which only allows reading of
 * information and setting the configuration by parsing strings.
 */
class BenchmarkConfiguration{
  // No real reason, to really build everything ourselves, when the
  // nlohmann::json object already containes everything, that we could need.
  nlohmann::json data_;

  public:

  /*
   * @brief Returns a value held in the configuration.
   *
   * @tparam ReturnType The type of the value, that you want to have later.
   * @tparam Index,Indexes The types of the keys, that you gave. Deduction
   *  should be able to handle it.
   * 
   * @param index,indexes The keys for getting the value out of the
   *  configuration. Look at the documentation of `nlohmann::basic_json::at`
   *  , if you want to see, what's possible.
   */
  template<typename ReturnType, typename Index, typename... Indexes>
  ReturnType getValue(const Index& index, const Indexes&... indexes) const{
    auto toReturn = data_.at(index);
    ((toReturn = toReturn.at(indexes)), ...);
    return toReturn.template get<ReturnType>();
  }

  /*
   * @brief Sets the configuration based on the given json string. This
   *  overwrites all previous held configuration data.
   */
  void parseJsonString(const std::string& jsonString);

  /*
   * @brief Parses the given short hand and adds all configuration data, that
   *  was described with valid syntax.
   *
   * @param shortHandString The valid syntax is: TODO Plan syntax.
   */
  void parseShortHand(const std::string& shortHandString);

  // JSON (d)serialization.
  // TODO This works, but `"data_":null` looks a bit ugly. Is there a way,
  // to skip the `data_` and just have the content of `data_` as the
  // serialied form of this class?
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(BenchmarkConfiguration, data_)
};
