//  Copyright 2023, University of Freiburg
//  Chair of Algorithms and Data Structures
//  Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//           Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/ChunkedForLoop.h"
#include "util/Random.h"

namespace sparqlExpression {

class RandomExpression : public SparqlExpression {
 private:
  // Unique random ID for this expression.
  int64_t randId = ad_utility::FastRandomIntGenerator<int64_t>{}();

 public:
  // Evaluate a Sparql expression.
  ExpressionResult evaluate(EvaluationContext* context) const override {
    VectorWithMemoryLimit<Id> result{context->_allocator};
    const size_t numElements = context->_endIndex - context->_beginIndex;
    result.reserve(numElements);
    ad_utility::FastRandomIntGenerator<int64_t> randInt;

    // As part of a GROUP BY we only return one value per group.
    if (context->_isPartOfGroupBy) {
      return Id::makeFromInt(randInt() >> Id::numDatatypeBits);
    }

    // 1000 is an arbitrarily chosen interval at which to check for
    // cancellation.
    ad_utility::chunkedForLoop<1000>(
        0, numElements,
        [&result, &randInt](size_t) {
          result.push_back(Id::makeFromInt(randInt() >> Id::numDatatypeBits));
        },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });
    return result;
  }

  // Get a unique identifier for this expression, used as cache key.
  string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return "RAND " + std::to_string(randId);
  }

 private:
  // Get the direct child expressions.
  std::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};

// FOR UUID EXPRESSION
using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;

inline constexpr auto fromLiteral = [](std::string_view str) {
  return LiteralOrIri::fromStringRepresentation(absl::StrCat("\"", str, "\""));
};

inline constexpr auto fromIri = [](std::string_view str) {
  return LiteralOrIri::fromStringRepresentation(
      absl::StrCat("<urn:uuid:", str, ">"));
};

inline constexpr auto litUuidKey = [](int64_t randId) {
  return "STRUUID " + std::to_string(randId);
};

inline constexpr auto iriUuidKey = [](int64_t randId) {
  return "UUID " + std::to_string(randId);
};

// With UuidExpression<fromIri, iriUuidKey>, the UUIDs are returned as an
// Iri object: <urn:uuid:b9302fb5-642e-4d3b-af19-29a8f6d894c9> (example). With
// UuidExpression<fromLiteral,, litUuidKey>, the UUIDs are returned as as an
// Literal object: "73cd4307-8a99-4691-a608-b5bda64fb6c1" (example).
template <auto FuncConv, auto FuncKey>
class UuidExpression : public SparqlExpression {
 private:
  int64_t randId = ad_utility::FastRandomIntGenerator<int64_t>{}();

 public:
  ExpressionResult evaluate(EvaluationContext* context) const override {
    VectorWithMemoryLimit<IdOrLiteralOrIri> result{context->_allocator};
    const size_t numElements = context->_endIndex - context->_beginIndex;
    result.reserve(numElements);
    ad_utility::UuidGenerator uuidGen;

    if (context->_isPartOfGroupBy) {
      return FuncConv(uuidGen());
    }

    ad_utility::chunkedForLoop<1000>(
        0, numElements,
        [&result, &uuidGen](size_t) { result.push_back(FuncConv(uuidGen())); },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });
    return result;
  }

  string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return FuncKey(randId);
  }

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};

}  // namespace sparqlExpression
