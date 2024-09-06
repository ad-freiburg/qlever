// Copyright 2022 - 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#pragma once

#include <functional>

#include "engine/Operation.h"
#include "engine/Values.h"
#include "parser/ParsedQuery.h"
#include "util/http/HttpClient.h"

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
      const ad_utility::httpUtils::Url&,
      ad_utility::SharedCancellationHandle handle,
      const boost::beast::http::verb&, std::string_view, std::string_view,
      std::string_view)>;

 private:
  // The parsed SERVICE clause.
  parsedQuery::Service parsedServiceClause_;

  // The function used to obtain the result from the remote endpoint.
  GetResultFunction getResultFunction_;

  // The siblingTree, used for SERVICE clause optimization.
  std::shared_ptr<QueryExecutionTree> siblingTree_;

 public:
  // Construct from parsed Service clause.
  //
  // NOTE: The third argument is the function used to obtain the result from the
  // remote endpoint. The default is to use `httpUtils::sendHttpOrHttpsRequest`,
  // but in our tests (`ServiceTest`) we use a mock function that does not
  // require a running `HttpServer`.
  Service(QueryExecutionContext* qec, parsedQuery::Service parsedServiceClause,
          GetResultFunction getResultFunction = sendHttpOrHttpsRequest,
          std::shared_ptr<QueryExecutionTree> siblingTree = nullptr);

  // Create a new `Service` operation, that is equal to `*this` but additionally
  // respects the `siblingTree`. The sibling tree is a partial query that will
  // later be joined with the result of the `Service`. If the result of the
  // sibling is small, it will be used to constrain the SERVICE query using a
  // `VALUES` clause.
  [[nodiscard]] std::shared_ptr<Service> createCopyWithSiblingTree(
      std::shared_ptr<QueryExecutionTree> siblingTree) const {
    AD_CORRECTNESS_CHECK(siblingTree_ == nullptr);
    // TODO<joka921> This copies the `parsedServiceClause_`. We could probably
    // use a `shared_ptr` here to reduce the copying during QueryPlanning.
    return std::make_shared<Service>(getExecutionContext(),
                                     parsedServiceClause_, getResultFunction_,
                                     std::move(siblingTree));
  }

  // Methods inherited from base class `Operation`.
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  std::vector<ColumnIndex> resultSortedOn() const override { return {}; }
  float getMultiplicity(size_t col) override;

  // Getters for testing.
  const auto& getSiblingTree() const { return siblingTree_; }
  const auto& getGraphPatternAsString() const {
    return parsedServiceClause_.graphPatternAsString_;
  }

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  size_t getCostEstimate() override;
  VariableToColumnMap computeVariableToColumnMap() const override;

  // We know nothing about the result at query planning time.
  bool knownEmptyResult() override { return false; }

  // A SERVICE clause has no children.
  vector<QueryExecutionTree*> getChildren() override { return {}; }

  // Convert the given binding to TripleComponent.
  static TripleComponent bindingToTripleComponent(const nlohmann::json& cell);

 private:
  // The string returned by this function is used as cache key.
  std::string getCacheKeyImpl() const override;

  // Compute the result using `getResultFunction_` and the siblingTree.
  ProtoResult computeResult([[maybe_unused]] bool requestLaziness) override;

  // Actually compute the result for the function above.
  ProtoResult computeResultImpl([[maybe_unused]] bool requestLaziness);

  // Get a VALUES clause that contains the values of the siblingTree's result.
  std::optional<std::string> getSiblingValuesClause() const;

  // Create result for silent fail.
  ProtoResult makeNeutralElementResultForSilentFail() const;

  // Write the given JSON result to the given result object. The `I` is the
  // width of the result table.
  //
  // NOTE: This is similar to `Values::writeValues`, except that we have to
  // parse JSON here and not a VALUES clause.
  template <size_t I>
  void writeJsonResult(const std::vector<std::string>& vars,
                       const std::vector<nlohmann::json>& bindings,
                       IdTable* idTable, LocalVocab* localVocab);
};
