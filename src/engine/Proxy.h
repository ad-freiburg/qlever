// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_PROXY_H
#define QLEVER_SRC_ENGINE_PROXY_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include "engine/Operation.h"
#include "parser/ProxyQuery.h"
#include "util/http/HttpClient.h"

// The operation for the magic `SERVICE qlproxy:`. Sends input bindings
// to a remote endpoint and receives output bindings back. For example,
//
//   SERVICE qlproxy: {
//     _:config qlproxy:endpoint <https://example.org/api> ;
//              qlproxy:input-first ?num1 ;
//              qlproxy:input-second ?num2 ;
//              qlproxy:output-result ?result ;
//              qlproxy:output-row ?row ;
//              qlproxy:param-operation "add" .
//   }
//
// This sends bindings for `?num1` as "first" and `?num2` as "second" to the
// given endpoint. The `qlproxy:param-...` values are sent as URL parameters,
// e.g., here `operation=add`. The service expects bindings for "result" in the
// response, which are mapped to `?result`.
//
// The `output-row` variable is used to join the response with the input rows.
// The input bindings include a 1-based row index (named after the row
// variable), and the endpoint must return this index in each output binding to
// specify which input row the output corresponds to.
//
// The input variables come from the enclosing graph pattern (sibling
// operations), which is added as a child of this operation by the query
// planner.
class Proxy : public Operation {
 private:
  // The configuration from the parsed query.
  parsedQuery::ProxyConfiguration config_;

  // The child operation that provides the input variable bindings.
  // This is set by the query planner when joining with sibling operations.
  std::optional<std::shared_ptr<QueryExecutionTree>> childOperation_;

  // The function used to send HTTP requests.
  SendRequestType sendRequestFunction_;

  // Counter to generate unique cache breaker IDs.
  static inline std::atomic_uint32_t counter_ = 0;
  uint32_t cacheBreaker_ = counter_++;

 public:
  // Construct from configuration. The child operation is optional and will be
  // added by the query planner when joining with sibling operations that
  // provide the input variables.
  Proxy(QueryExecutionContext* qec, parsedQuery::ProxyConfiguration config,
        std::optional<std::shared_ptr<QueryExecutionTree>> childOperation =
            std::nullopt,
        SendRequestType sendRequestFunction = sendHttpOrHttpsRequest);

  // Add a child operation that provides the input variable bindings.
  // Returns a new `Proxy` with the child added.
  std::shared_ptr<Proxy> addChild(
      std::shared_ptr<QueryExecutionTree> child) const;

  // Check if the proxy is fully constructed (has a child, or doesn't need one).
  bool isConstructed() const {
    return childOperation_.has_value() || config_.inputVariables_.empty();
  }

  // Methods inherited from Operation.
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  std::vector<ColumnIndex> resultSortedOn() const override { return {}; }
  float getMultiplicity(size_t col) override;
  size_t getCostEstimate() override;
  uint64_t getSizeEstimateBeforeLimit() override;
  VariableToColumnMap computeVariableToColumnMap() const override;
  bool knownEmptyResult() override { return false; }
  std::vector<QueryExecutionTree*> getChildren() override;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;
  std::string getCacheKeyImpl() const override;
  Result computeResult(bool requestLaziness) override;

  // Serialize input bindings from the child result as SPARQL Results JSON.
  // Includes row indices for joining.
  std::string serializeInputAsJson(const Result& childResult) const;

  // Build the URL with query parameters.
  std::string buildUrlWithParams() const;

  // Join the proxy response with the child result based on the row variable.
  IdTable joinResponseWithChild(const IdTable& responseTable,
                                ColumnIndex responseRowCol,
                                const IdTable& childTable,
                                const LocalVocab& childLocalVocab,
                                LocalVocab& resultLocalVocab) const;
};

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_PROXY_H
