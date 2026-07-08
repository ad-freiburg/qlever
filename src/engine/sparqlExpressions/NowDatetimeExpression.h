//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NOWDATETIMEEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NOWDATETIMEEXPRESSION_H

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/DateYearDuration.h"

namespace qlever::sparqlExpression {

// The expression `NOW()` is evaluated within NowDatetimeExpression.h.
// `NowDatetimeExpression` has to be explicitly constructed from a
// `date-formatted string`, which is for all evaluations within a Sparql
// query the same.
class NowDatetimeExpression : public SparqlExpression {
 private:
  DateYearOrDuration date_;

 public:
  explicit NowDatetimeExpression(const std::string& dateTimeFormat)
      : date_(DateYearOrDuration::parseXsdDatetime(dateTimeFormat)) {}

  std::string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return absl::StrCat("NOW ", date_.toBits());
  }

  ExpressionResult evaluate(
      [[maybe_unused]] EvaluationContext* context) const override {
    return Id::makeFromDate(date_);
  }

  // Since the result of `NOW()` is determined at construction time, it is
  // deterministic as long as the cache key is the same which depends on the
  // time stored in `date_`.
  [[nodiscard]] bool isDeterministic() const override { return true; }

 private:
  ql::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};

}  // namespace qlever::sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NOWDATETIMEEXPRESSION_H
