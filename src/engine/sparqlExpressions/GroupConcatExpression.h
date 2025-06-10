// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_GROUPCONCATEXPRESSION_H
#define QLEVER_GROUPCONCATEXPRESSION_H

#include "engine/sparqlExpressions/AggregateExpression.h"

namespace sparqlExpression {
/// The GROUP_CONCAT Expression
class GroupConcatExpression : public SparqlExpression {
 private:
  Ptr child_;
  std::string separator_;
  bool distinct_;

 public:
  GroupConcatExpression(bool distinct, Ptr&& child, std::string separator);

  ExpressionResult evaluate(EvaluationContext* context) const override;

  // Required when using the hash map optimization.
  const std::string& getSeparator() const { return separator_; }

  // A `GroupConcatExpression` is an aggregate.
  AggregateStatus isAggregate() const override {
    return distinct_ ? AggregateStatus::DistinctAggregate
                     : AggregateStatus::NonDistinctAggregate;
  }

  [[nodiscard]] std::string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

 private:
  // ___________________________________________________________________________
  ql::span<Ptr> childrenImpl() override { return {&child_, 1}; }
};
}  // namespace sparqlExpression

#endif  // QLEVER_GROUPCONCATEXPRESSION_H
