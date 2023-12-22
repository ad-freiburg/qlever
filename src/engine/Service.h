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
// service IRI, gets the result as TSV, parses it, and writes it into a result
// table.
//
// TODO: The current implementation works, but is preliminary in several
// respects:
//
// 1. Reading the result as TSV has potential problems (see comment in
// `computeResult` for details).
//
// 2. There should be a timeout.
//
// 3. A variable in place of the IRI is not yet supported (see comment in
// `computeResult` for details).
//
// 4. The SERVICE is currently executed *after* the query planning. The
// estimates of the result size, cost, and multiplicities are therefore dummy
// values.
//
class Service : public Operation {
 public:
  // The type of the function used to obtain the results, see below.
  using GetTsvFunction = std::function<std::istringstream(
      ad_utility::httpUtils::Url, const boost::beast::http::verb&,
      std::string_view, std::string_view, std::string_view)>;

 private:
  // The parsed SERVICE clause.
  parsedQuery::Service parsedServiceClause_;

  // The function used to obtain the result from the remote endpoint.
  GetTsvFunction getTsvFunction_;

 public:
  // Construct from parsed Service clause.
  //
  // NOTE: The third argument is the function used to obtain the result from the
  // remote endpoint. The default is to use `httpUtils::sendHttpOrHttpsRequest`,
  // but in our tests (`ServiceTest`) we use a mock function that does not
  // require a running `HttpServer`.
  Service(QueryExecutionContext* qec, parsedQuery::Service parsedServiceClause,
          GetTsvFunction getTsvFunction = sendHttpOrHttpsRequest);

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

  // Not relevant for SERVICE.
  void setTextLimit([[maybe_unused]] size_t limit) override {}

  // We know nothing about the result at query planning time.
  bool knownEmptyResult() override { return false; }

  // A SERVICE clause has no children.
  vector<QueryExecutionTree*> getChildren() override { return {}; }

 private:
  // The string returned by this function is used as cache key.
  std::string getCacheKeyImpl() const override;

  // Compute the result using `getTsvFunction_`.
  ResultTable computeResult() override;

  // Write the given TSV result to the given result object. The `I` is the width
  // of the result table.
  //
  // NOTE: This is similar to `Values::writeValues`, except that we have to
  // parse TSV here and not a VALUES clause. Note that the only reason that
  // `tsvResult` is not `const` here is because the method iterates over the
  // `std::istringstream` and thus changes it.
  template <size_t I>
  void writeTsvResult(std::istringstream tsvResult, IdTable* idTable,
                      LocalVocab* localVocab);
};
