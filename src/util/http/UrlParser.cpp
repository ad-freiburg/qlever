// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "UrlParser.h"

using namespace ad_utility::url_parser;

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
    if (result.contains(key)) {
      result[key].push_back(value);
    } else {
      auto [blockingElement, wasInserted] = result.insert({key, {value}});
      if (!wasInserted) {
        AD_FAIL();
      }
    }
  }
  return result;
}

// _____________________________________________________________________________
std::vector<SparqlQleverVisitor::DatasetClause>
ad_utility::url_parser::parseDatasetClauses(const ParamValueMap& params) {
  std::vector<SparqlQleverVisitor::DatasetClause> datasetClauses;
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
