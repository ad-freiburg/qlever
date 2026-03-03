// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_LOAD_H
#define QLEVER_LOAD_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <string>

#include "engine/Operation.h"
#include "parser/ParsedQuery.h"
#include "util/http/HttpClient.h"

// This class implements the SPARQL UPDATE `LOAD` operation. It reads a turtle
// document from a remote URL via HTTP and converts it to an `IdTable`.
class Load final : public Operation {
 public:
  static constexpr std::array<ad_utility::MediaType, 2> SUPPORTED_MEDIATYPES{
      ad_utility::MediaType::turtle, ad_utility::MediaType::ntriples};

 private:
  // The generated LOAD clause.
  parsedQuery::Load loadClause_;

  // The function used to obtain the result from the remote endpoint.
  SendRequestType getResultFunction_;

  // Counter to generate fresh ids for each instance of the class.
  static inline std::atomic_uint32_t counter_ = 0;

  // Id that is used to avoid caching of the result. It is unique for every
  // instance of the class.
  uint32_t cacheBreaker_ = counter_++;

  // Initialized to the value of the runtime parameter `cache-load-results` at
  // construction.
  bool loadResultCachingEnabled_;

 public:
  Load(QueryExecutionContext* qec, parsedQuery::Load loadClause,
       SendRequestType getResultFunction = sendHttpOrHttpsRequest);

  ~Load() override = default;

  std::vector<QueryExecutionTree*> getChildren() override { return {}; }

  bool canResultBeCachedImpl() const override;

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
  std::vector<ColumnIndex> resultSortedOn() const override;

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

 public:
  // Allows overriding the `getResultFunction_` for testing purposes.
  void resetGetResultFunctionForTesting(SendRequestType func);
};

#endif
#endif  // QLEVER_LOAD_H
