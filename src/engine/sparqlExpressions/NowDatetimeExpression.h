//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/ChunkedForLoop.h"
#include "util/Date.h"
#include "util/Random.h"

namespace sparqlExpression {

// The expression NOW() is evaluated within NowDatetimeExpression.
// NowDatetimeExpression has to be explicitly instantiated with a
// date-formatted string, which is for all evaluations within a Sparql query the
// same.
class NowDatetimeExpression : public SparqlExpression {
 private:
  int64_t randId_ = ad_utility::FastRandomIntGenerator<int64_t>{}();
  DateOrLargeYear date_;

 public:
  explicit NowDatetimeExpression(const std::string& dateTimeFormat)
      : date_(DateOrLargeYear::parseXsdDatetime(dateTimeFormat)) {}

  std::string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return absl::StrCat("NOW ", std::to_string(randId_));
  }

  ExpressionResult evaluate(EvaluationContext* context) const override {
    VectorWithMemoryLimit<Id> result{context->_allocator};
    DateOrLargeYear date = date_;
    const size_t numElements = context->_endIndex - context->_beginIndex;
    result.reserve(numElements);

    if (context->_isPartOfGroupBy) {
      return Id::makeFromDate(date_);
    }

    ad_utility::chunkedForLoop<1000>(
        0, numElements,
        [&result, &date](size_t) { result.push_back(Id::makeFromDate(date)); },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });

    return result;
  }

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};

}  // namespace sparqlExpression
