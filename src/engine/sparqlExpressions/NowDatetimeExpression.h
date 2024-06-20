//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/ChunkedForLoop.h"
#include "util/Date.h"
#include "util/Random.h"

namespace sparqlExpression {

// The expression `NOW()` is evaluated within NowDatetimeExpression.h.
// `NowDatetimeExpression` has to be explicitly constructed from a
// `date-formatted string`, which is for all evaluations within a Sparql
// query the same.
class NowDatetimeExpression : public SparqlExpression {
 private:
  DateOrLargeYear date_;

 public:
  explicit NowDatetimeExpression(const std::string& dateTimeFormat)
      : date_(DateOrLargeYear::parseXsdDatetime(dateTimeFormat)) {}

  std::string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return absl::StrCat("NOW ", date_.toBits());
  }

  ExpressionResult evaluate(
      [[maybe_unused]] EvaluationContext* context) const override {
    return Id::makeFromDate(date_);
  }

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};

}  // namespace sparqlExpression
