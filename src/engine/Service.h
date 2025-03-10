// Copyright 2022 - 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#pragma once

#include <functional>

#include "engine/Operation.h"
#include "engine/VariableToColumnMap.h"
#include "parser/ParsedQuery.h"
#include "util/LazyJsonParser.h"
#include "util/http/HttpClient.h"
#include "util/http/websocket/WebSocketClient.h"

// The SERVICE operation. Sends a query to the remote endpoint specified by the
// service IRI, gets the result as JSON, parses it, and writes it into a result
// table.
//
// TODO: The current implementation works, but is preliminary in several
// respects:
//
// 1. There should be a timeout.
//
// 2. A variable in place of the IRI is not yet supported (see comment in
// `computeResult` for details).
//
// 3. The SERVICE is currently executed *after* the query planning. The
// estimates of the result size, cost, and multiplicities are therefore dummy
// values.
//
class Service : public Operation {
 public:
  // The type of the function used to obtain the results, see below.
  using GetResultFunction = std::function<HttpOrHttpsResponse(
      const ad_utility::httpUtils::Url&, ad_utility::SharedCancellationHandle,
      const boost::beast::http::verb&, std::string_view, std::string_view,
      std::string_view,
      const std::vector<std::pair<std::string, std::string>>&)>;

  // The type of the function used to obtain the RuntimeInformation.
  using WebSocketClientVariant = std::variant<
      std::unique_ptr<ad_utility::websocket::HttpWebSocketClient>,
      std::unique_ptr<ad_utility::websocket::HttpsWebSocketClient>>;
  using GetRuntimeInfoClient = std::function<WebSocketClientVariant(
      const ad_utility::httpUtils::Url&, const std::string& target,
      std::function<void(const std::string&)>)>;

  // Information on a Sibling operation.
  struct SiblingInfo {
    std::shared_ptr<const Result> precomputedResult_;
    VariableToColumnMap variables_;
    std::string cacheKey_;
  };

  struct NetworkFunctions {
    GetResultFunction getResultFunction_;
    GetRuntimeInfoClient getRuntimeInfoClient_;
  };

 private:
  // The parsed SERVICE clause.
  parsedQuery::Service parsedServiceClause_;

  // The functions used to obtain the result and runtime information from the
  // remote endpoint.
  NetworkFunctions networkFunctions_;

  // Optional sibling information to be used in `getSiblingValuesClause`.
  std::optional<SiblingInfo> siblingInfo_;

  // RuntimeInformation of the service-query computation on the endpoint.
  std::shared_ptr<RuntimeInformation> childRuntimeInformation_;

 public:
  // Construct from parsed Service clause.
  //
  // NOTE: The third argument is the function used to obtain the result from the
  // remote endpoint. The default is to use `httpUtils::sendHttpOrHttpsRequest`,
  // but in our tests (`ServiceTest`) we use a mock function that does not
  // require a running `HttpServer`.
  Service(QueryExecutionContext* qec, parsedQuery::Service parsedServiceClause,
          NetworkFunctions networkFunctions = {
              .getResultFunction_ = sendHttpOrHttpsRequest,
              .getRuntimeInfoClient_ =
                  ad_utility::websocket::getRuntimeInfoClient});

  // Methods inherited from base class `Operation`.
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  std::vector<ColumnIndex> resultSortedOn() const override { return {}; }
  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  size_t getCostEstimate() override;
  VariableToColumnMap computeVariableToColumnMap() const override;

  // We know nothing about the result at query planning time.
  bool knownEmptyResult() override { return false; }

  // A SERVICE clause has no children.
  vector<QueryExecutionTree*> getChildren() override { return {}; }

  absl::InlinedVector<std::shared_ptr<RuntimeInformation>, 2>
  getRuntimeInfoChildren() final {
    if (childRuntimeInformation_) {
      return {childRuntimeInformation_};
    }
    return {};
  }

  // Convert the given binding to TripleComponent.
  TripleComponent bindingToTripleComponent(
      const nlohmann::json& binding,
      ad_utility::HashMap<std::string, Id>& blankNodeMap,
      LocalVocab* localVocab) const;

  // Create a value for the VALUES-clause used in `getSiblingValuesClause` from
  // id. If the id is of type blank node `std::nullopt` is returned.
  static std::optional<std::string> idToValueForValuesClause(
      const Index& index, Id id, const LocalVocab& localVocab);

  // Given two child-operations of a `Join`-, `OptionalJoin`- or `Minus`
  // operation, this method tries to precompute the result of one if the other
  // one (its sibling) is a `Service` operation. If `rightOnly` is true (used by
  // `OptionalJoin` and `Minus`), only the right operation can be a `Service`.
  static void precomputeSiblingResult(std::shared_ptr<Operation> left,
                                      std::shared_ptr<Operation> right,
                                      bool rightOnly, bool requestLaziness);

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  // The string returned by this function is used as cache key.
  std::string getCacheKeyImpl() const override;

  // Compute the result using `getResultFunction_` and `siblingInfo_`.
  ProtoResult computeResult([[maybe_unused]] bool requestLaziness) override;

  // Actually compute the result for the function above.
  ProtoResult computeResultImpl([[maybe_unused]] bool requestLaziness);

  // Get a VALUES clause that contains the values of the siblingTree's result.
  std::optional<std::string> getSiblingValuesClause() const;

  // Create result for silent fail.
  ProtoResult makeNeutralElementResultForSilentFail() const;

  // Check that all visible variables of the SERVICE clause exist in the json
  // object, otherwise throw an error.
  void verifyVariables(const nlohmann::json& head,
                       const ad_utility::LazyJsonParser::Details& gen) const;

  // Throws an error message, providing the first 100 bytes of the result as
  // context.
  [[noreturn]] void throwErrorWithContext(
      std::string_view msg, std::string_view first100,
      std::string_view last100 = ""sv) const;

  // Write the given JSON result to the given result object. The `I` is the
  // width of the result table.
  //
  // NOTE: This is similar to `Values::writeValues`, except that we have to
  // parse JSON here and not a VALUES clause.
  template <size_t I>
  void writeJsonResult(const std::vector<std::string>& vars,
                       const nlohmann::json& partJson, IdTable* idTable,
                       LocalVocab* localVocab, size_t& rowIdx);

  // Compute the result lazy as IdTable generator.
  // If the `singleIdTable` flag is set, the result is yielded as one idTable.
  Result::Generator computeResultLazily(
      const std::vector<std::string> vars,
      ad_utility::LazyJsonParser::Generator body, bool singleIdTable);

  void handleChildRuntimeInfoUpdate(const std::string& msg);

  FRIEND_TEST(ServiceTest, computeResult);
  FRIEND_TEST(ServiceTest, getCacheKey);
  FRIEND_TEST(ServiceTest, precomputeSiblingResult);
  FRIEND_TEST(ServiceTest, ChildRuntimeInformation);
};
