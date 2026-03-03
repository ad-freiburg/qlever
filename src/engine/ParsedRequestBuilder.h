// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_PARSEDREQUESTBUILDER_H
#define QLEVER_SRC_ENGINE_PARSEDREQUESTBUILDER_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include "util/http/UrlParser.h"
#include "util/http/beast.h"

// Helper for parsing `HttpRequest` into `ParsedRequest`. The parsing has many
// common patterns but the details are slightly different. This struct
// stores the partially parsed `ParsedRequest` and methods for common
// operations used while parsing.
struct ParsedRequestBuilder {
  FRIEND_TEST(ParsedRequestBuilderTest, extractTargetGraph);
  FRIEND_TEST(ParsedRequestBuilderTest, determineAccessToken);
  FRIEND_TEST(ParsedRequestBuilderTest, parameterIsContainedExactlyOnce);

  using RequestType =
      boost::beast::http::request<boost::beast::http::string_body>;

  ad_utility::url_parser::ParsedRequest parsedRequest_;

  // Graph Store Protocol direct graph identification needs the host to be able
  // to determine the graph IRI.
  std::optional<std::string> host_ = std::nullopt;

  // Initialize a `ParsedRequestBuilder`, parsing the request target into the
  // `ParsedRequest`.
  explicit ParsedRequestBuilder(const RequestType& request);

  // Extract the access token from the access-token parameter or the
  // Authorization header and set it for `ParsedRequest`. If both are given,
  // then they must be the same.
  void extractAccessToken(const RequestType& request);

  // If applicable extract the dataset clauses from the parameters and set them
  // on the Query or Update.
  void extractDatasetClauses();

  // If the parameter is set, set the operation with the parameter's value as
  // operation string and empty dataset clauses. Setting an operation when one
  // is already set is an error. Note: processed parameters are removed from the
  // parameter map.
  template <typename Operation>
  void extractOperationIfSpecified(std::string_view paramName);

  // Returns whether the request is a Graph Store operation.
  bool isGraphStoreOperationIndirect() const;
  bool isGraphStoreOperationDirect() const;

  // Set the operation to the parsed Graph Store operation.
  void extractGraphStoreOperationIndirect();
  void extractGraphStoreOperationDirect();

  // Returns whether the parameters contain a parameter with the given key.
  bool parametersContain(std::string_view param) const;

  // Check that requests don't both have these content types and are Graph
  // Store operations.
  void reportUnsupportedContentTypeIfGraphStore(
      std::string_view contentType) const;

  // Move the `ParsedRequest` out when parsing is finished.
  ad_utility::url_parser::ParsedRequest build() &&;

 private:
  // Adds a dataset clause to the operation if it is of the given type. The
  // dataset clause's IRI is the value of parameter `key`. The `isNamed_` of the
  // dataset clause is as given.
  template <typename Operation>
  void extractDatasetClauseIfOperationIs(const std::string& key, bool isNamed);

  // Check that a parameter is contained exactly once. An exception is thrown if
  // a parameter is contained more than once.
  bool parameterIsContainedExactlyOnce(std::string_view key) const;

  // Extract the graph to be acted upon using from the URL query parameters
  // (`Indirect Graph Identification`). See
  // https://www.w3.org/TR/2013/REC-sparql11-http-rdf-update-20130321/#indirect-graph-identification
  static GraphOrDefault extractTargetGraph(
      const ad_utility::url_parser::ParamValueMap& params);

  // Determine the access token from the parameters and the requests
  // Authorization header.
  static std::optional<std::string> determineAccessToken(
      const RequestType& request,
      const ad_utility::url_parser::ParamValueMap& params);
};

#endif
#endif  // QLEVER_SRC_ENGINE_PARSEDREQUESTBUILDER_H
