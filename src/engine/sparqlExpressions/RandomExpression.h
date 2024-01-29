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
        [context]() {
          context->cancellationHandle_->throwIfCancelled("RandomExpression");
        });
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

}  // namespace sparqlExpression
