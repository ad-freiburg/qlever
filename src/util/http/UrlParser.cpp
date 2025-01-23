// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "UrlParser.h"

using namespace ad_utility::url_parser;

// _____________________________________________________________________________
std::optional<std::string> ad_utility::url_parser::getParameterCheckAtMostOnce(
    const ParamValueMap& map, string_view key) {
  if (!map.contains(key)) {
    return std::nullopt;
  }
  auto& value = map.at(key);
  if (value.size() != 1) {
    throw std::runtime_error(
        absl::StrCat("Parameter \"", key,
                     "\" must be given exactly once. Is: ", value.size()));
  }
  return value.front();
}

// _____________________________________________________________________________
std::optional<std::string> ad_utility::url_parser::checkParameter(
    const ParamValueMap& parameters, std::string_view key,
    std::optional<std::string> value) {
  const auto param = getParameterCheckAtMostOnce(parameters, key);
  if (!param.has_value()) {
    return std::nullopt;
  }
  std::string parameterValue = param.value();

  // If no value is given, return the parameter's value. If value is given, but
  // not equal to the parameter's value, return `std::nullopt`.
  if (value == std::nullopt) {
    value = parameterValue;
  } else if (value != parameterValue) {
    return std::nullopt;
  }
  return value;
}

// _____________________________________________________________________________
ParsedUrl ad_utility::url_parser::parseRequestTarget(std::string_view target) {
  auto urlResult = boost::urls::parse_origin_form(target);
  if (urlResult.has_error()) {
    throw std::runtime_error(
        absl::StrCat("Failed to parse URL: \"", target, "\"."));
  }
  boost::url url = urlResult.value();

  return {url.path(), paramsToMap(url.params())};
}

// _____________________________________________________________________________
ParamValueMap ad_utility::url_parser::paramsToMap(
    boost::urls::params_view params) {
  ParamValueMap result;
  for (const auto& [key, value, _] : params) {
    result[key].push_back(value);
  }
  return result;
}

// _____________________________________________________________________________
std::vector<DatasetClause> ad_utility::url_parser::parseDatasetClauses(
    const ParamValueMap& params) {
  std::vector<DatasetClause> datasetClauses;
  auto readDatasetClauses = [&params, &datasetClauses](const std::string& key,
                                                       bool isNamed) {
    if (params.contains(key)) {
      for (const auto& uri : params.at(key)) {
        datasetClauses.emplace_back(
            ad_utility::triple_component::Iri::fromIrirefWithoutBrackets(uri),
            isNamed);
      }
    }
  };
  readDatasetClauses("default-graph-uri", false);
  readDatasetClauses("named-graph-uri", true);
  return datasetClauses;
}
