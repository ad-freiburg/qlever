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
using Literal = ad_utility::triple_component::Literal;
using Iri = ad_utility::triple_component::Iri;

inline constexpr auto toLiteral = [](std::string_view str) {
  return LiteralOrIri{
      Literal::literalWithNormalizedContent(asNormalizedStringViewUnsafe(str))};
};

inline constexpr auto toIri = [](std::string_view str) {
  return LiteralOrIri{
      Iri::fromStringRepresentation(absl::StrJoin("<", "urn:uuid:", str, ">"))};
};

inline constexpr auto litUuidKey = [](int64_t randId) {
  return "STRUUID" + std::to_string(randId);
};

inline constexpr auto iriUuidKey = [](int64_t randId) {
  return "UUID" + std::to_string(randId);
};

template <auto FuncConv, auto FuncKey>
class UuidExpression : public SparqlExpression {
 private:
  int64_t randId = ad_utility::FastRandomIntGenerator<int64_t>{}();

 public:
  ExpressionResult evaluate(EvaluationContext* context) const override {
    VectorWithMemoryLimit<IdOrLiteralOrIri> result{context->_allocator};
    const size_t numElements = context->_endIndex - context->_beginIndex;
    result.reserve(numElements);
    ad_utility::UuidGenerator uuidGen = {};

    if (context->_isPartOfGroupBy) {
      return FuncConc(uuidGen());
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

UuidExpression<toLiteral, litUuidKey> StrUuid;
UuidExpression<toIri, iriUuidKey> Uuid;

}  // namespace sparqlExpression
