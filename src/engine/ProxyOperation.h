// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_PROXYOPERATION_H
#define QLEVER_SRC_ENGINE_PROXYOPERATION_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include "engine/Operation.h"
#include "parser/ProxyQuery.h"
#include "util/http/HttpClient.h"

// The operation for the magic `SERVICE qlproxy:`. Sends payload bindings
// to a remote endpoint and receives result bindings back. For example,
//
//   SERVICE qlproxy: {
//     _:config qlproxy:endpoint <https://example.org/api> ;
//              qlproxy:payload_first ?num1 ;
//              qlproxy:payload_second ?num2 ;
//              qlproxy:result_res ?result ;
//              qlproxy:param_operation "add" .
//   }
//
// This sends bindings for `?num1` as "first" and `?num2` as "second" to the
// given endpoint. The `qlproxy:param_...` values are sent as URL parameters,
// e.g., here `operation=add`. The service expects bindings for "res" in the
// response, which are mapped to `?result`.
//
// The payload variables come from the enclosing graph pattern (sibling
// operations), which is added as a child of this operation by the query
// planner.
class ProxyOperation : public Operation {
 private:
  // The configuration from the parsed query.
  parsedQuery::ProxyConfiguration config_;

  // The child operation that provides the payload variable bindings.
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
  // provide the payload variables.
  ProxyOperation(QueryExecutionContext* qec,
                 parsedQuery::ProxyConfiguration config,
                 std::optional<std::shared_ptr<QueryExecutionTree>>
                     childOperation = std::nullopt,
                 SendRequestType sendRequestFunction = sendHttpOrHttpsRequest);

  // Add a child operation that provides the payload variable bindings.
  // Returns a new `ProxyOperation` with the child added.
  std::shared_ptr<ProxyOperation> addChild(
      std::shared_ptr<QueryExecutionTree> child) const;

  // Check if the proxy is fully constructed (has a child, or doesn't need one).
  bool isConstructed() const {
    return childOperation_.has_value() || config_.payloadVariables_.empty();
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

  // Serialize payload bindings from the child result as SPARQL Results JSON.
  std::string serializePayloadAsJson(const Result& childResult) const;

  // Build the URL with query parameters.
  std::string buildUrlWithParams() const;
};

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_PROXYOPERATION_H
