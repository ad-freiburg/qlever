// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_LOADURL_H
#define QLEVER_LOADURL_H

#include <string>

#include "engine/Operation.h"
#include "parser/ParsedQuery.h"
#include "util/http/HttpClient.h"

// This class implements the SPARQL UPDATE `LOAD` operation. It reads a turtle
// document from a remote URL via HTTP and converts it to an `IdTable`.
class LoadURL final : public Operation {
 public:
  // The type of the function used to obtain the results, see below.
  using GetResultFunction = std::function<HttpOrHttpsResponse(
      const ad_utility::httpUtils::Url&,
      ad_utility::SharedCancellationHandle handle,
      const boost::beast::http::verb&, std::string_view, std::string_view,
      std::string_view)>;

  const std::vector<ad_utility::MediaType> SUPPORTED_MEDIATYPES{
      ad_utility::MediaType::turtle, ad_utility::MediaType::ntriples};

 private:
  // The generated LOAD URL clause.
  parsedQuery::LoadURL loadURLClause_;

  // The function used to obtain the result from the remote endpoint.
  GetResultFunction getResultFunction_;

  // Counter to generate fresh ids for each instance of the class.
  static inline std::atomic_uint32_t counter_ = 0;

  // Id that is used to avoid caching of the result. It is unique for every
  // instance of the class.
  uint32_t cacheBreaker_ = counter_++;

 public:
  LoadURL(QueryExecutionContext* qec, parsedQuery::LoadURL loadURLClause,
          GetResultFunction getResultFunction = sendHttpOrHttpsRequest)
      : Operation(qec),
        loadURLClause_(loadURLClause),
        getResultFunction_(std::move(getResultFunction)){};

  ~LoadURL() override = default;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  bool canResultBeCached() const;

  std::string getCacheKeyImpl() const override;

  std::string getDescriptor() const override;

  size_t getResultWidth() const override;

  size_t getCostEstimate() override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  float getMultiplicity(size_t col) override;

  bool knownEmptyResult() override;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

 protected:
  vector<ColumnIndex> resultSortedOn() const override;

 private:
  // Error handling around `computeResultImpl`.
  Result computeResult(bool requestLaziness) override;
  // Actually compute the result for `computeResult()`.
  Result computeResultImpl(bool requestLaziness);

  VariableToColumnMap computeVariableToColumnMap() const override;

  // Throws an error message, providing the first 100 bytes of the result as
  // context.
  [[noreturn]] void throwErrorWithContext(
      std::string_view msg, std::string_view first100,
      std::string_view last100 = ""sv) const;
};

#endif  // QLEVER_LOADURL_H
