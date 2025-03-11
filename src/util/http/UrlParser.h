// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#ifndef QLEVER_URLPARSER_H
#define QLEVER_URLPARSER_H

#include <boost/url.hpp>
#include <string>
#include <string_view>

#include "parser/data/GraphRef.h"
#include "parser/sparqlParser/DatasetClause.h"
#include "util/HashMap.h"

/**
 * /brief Some helpers to parse request URLs in QLever.
 */
namespace ad_utility::url_parser {

// A map that stores the values for parameters. Parameters can be specified
// multiple times with different values.
using ParamValueMap = ad_utility::HashMap<string, std::vector<string>>;

// Extracts a parameter that must be present exactly once. If the parameter is
// not present std::nullopt is returned. If the parameter is present multiple
// times an exception is thrown.
std::optional<std::string> getParameterCheckAtMostOnce(const ParamValueMap& map,
                                                       string_view key);

// Checks if a parameter exists, and it matches the
// expected `value`. If yes, return the value, otherwise return
// `std::nullopt`.
std::optional<std::string> checkParameter(const ParamValueMap& parameters,
                                          std::string_view key,
                                          std::optional<std::string> value);

// A parsed URL.
// - `path_` is the URL path
// - `parameters_` is a map of the HTTP Query parameters
struct ParsedUrl {
  std::string path_;
  ParamValueMap parameters_;
};

// The different SPARQL operations that a `ParsedRequest` can represent. The
// operations represent the detected operation type and can contain additional
// information that the operation needs.
namespace sparqlOperation {
// A SPARQL 1.1 Query
struct Query {
  std::string query_;
  std::vector<DatasetClause> datasetClauses_;

  bool operator==(const Query& rhs) const = default;
};

// A SPARQL 1.1 Update
struct Update {
  std::string update_;
  std::vector<DatasetClause> datasetClauses_;

  bool operator==(const Update& rhs) const = default;
};

// A Graph Store HTTP Protocol operation. We only store the graph on which the
// operation acts. The actual operation is extracted later.
struct GraphStoreOperation {
  GraphOrDefault graph_;
  bool operator==(const GraphStoreOperation& rhs) const = default;
};

// No operation. This can happen for QLever's custom operations (e.g.
// `cache-stats`). These requests have no operation but are still valid.
struct None {
  bool operator==(const None& rhs) const = default;
};

using Operation = std::variant<Query, Update, GraphStoreOperation, None>;
}  // namespace sparqlOperation

// Representation of parsed HTTP request.
// - `path_` is the URL path
// - `accessToken_` is the access token for that request
// - `parameters_` is a hashmap of the parameters
// - `operation_` the operation that should be performed
struct ParsedRequest {
  std::string path_;
  std::optional<std::string> accessToken_;
  ParamValueMap parameters_;
  sparqlOperation::Operation operation_;
};

// Parse the URL path and the URL query parameters of an HTTP Request target.
ParsedUrl parseRequestTarget(std::string_view target);

// Convert the HTTP Query parameters `params` to a ParamValueMap (a map from
// string to vectors of strings).
ParamValueMap paramsToMap(boost::urls::params_view params);

// Parse the dataset clauses from the given key in the parameters.
std::vector<DatasetClause> parseDatasetClausesFrom(const ParamValueMap& params,
                                                   const std::string& key,
                                                   bool isNamed);

}  // namespace ad_utility::url_parser

#endif  // QLEVER_URLPARSER_H
