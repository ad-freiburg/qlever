//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <memory>
#include <span>
#include <vector>

#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "parser/data/Variable.h"

namespace sparqlExpression {

// Virtual base class for an arbitrary Sparql Expression which holds the
// structure of the expression as well as the logic to evaluate this expression
// on a given intermediate result
class SparqlExpression {
 private:
  std::string _descriptor;
  bool isInsideAggregate_ = false;

 public:
  // ________________________________________________________________________
  using Ptr = std::unique_ptr<SparqlExpression>;

  // Evaluate a Sparql expression.
  virtual ExpressionResult evaluate(EvaluationContext*) const = 0;

  // Return all variables, needed for certain parser methods.
  virtual std::vector<const Variable*> containedVariables() const final;

  // Return all the variables that occur in the expression, but are not
  // aggregated. These variables must be grouped in a GROUP BY. The default
  // implementation works for aggregate expressions (which never have
  // unaggregated variables) and for expressions that only combine other
  // expressions and therefore propagate their unaggregated variables. Leaf
  // operations (in particular the `VariableExpression`) need to override this
  // method.
  virtual std::vector<Variable> getUnaggregatedVariables() const;

  // Return true iff this expression contains an aggregate like SUM, COUNT etc.
  // This information is needed to check if there is an implicit GROUP BY in a
  // query because any of the selected aliases contains an aggregate.
  virtual bool containsAggregate() const final;

  // Check if expression is an aggregate. If true, then the return type also
  // specifies, whether the aggregate is `DISTINCT` or not.
  enum struct AggregateStatus {
    NoAggregate,
    DistinctAggregate,
    NonDistinctAggregate
  };
  virtual AggregateStatus isAggregate() const;

  // Replace child at index `childIndex` with `newExpression`. Return the old
  // child.
  virtual std::unique_ptr<SparqlExpression> replaceChild(
      size_t childIndex, std::unique_ptr<SparqlExpression> newExpression);

  // Get a unique identifier for this expression, used as cache key.
  virtual string getCacheKey(const VariableToColumnMap& varColMap) const = 0;

  // Get a short, human-readable identifier for this expression.
  virtual const string& descriptor() const final;
  virtual string& descriptor() final;

  // For the pattern trick we need to know, whether this expression
  // is a non-distinct count of a single variable. In this case we return
  // the variable. Otherwise we return std::nullopt.
  virtual std::optional<SparqlExpressionPimpl::VariableAndDistinctness>
  getVariableForCount() const;

  // Helper function for getVariableForCount() : If this
  // expression is a single variable, return the name of this variable.
  // Otherwise, return std::nullopt.
  virtual std::optional<::Variable> getVariableOrNullopt() const;

  // For the following three functions (`containsLangExpression`,
  // `getLanguageFilterExpression`, and `getEstimatesForFilterExpression`, see
  // the documentation of the functions of the same names in
  // `SparqlExpressionPimpl.h`. Each of them has a default implementation that
  // is correct for most of the expressions.
  virtual bool containsLangExpression() const;

  // ___________________________________________________________________________
  using LangFilterData = SparqlExpressionPimpl::LangFilterData;
  virtual std::optional<LangFilterData> getLanguageFilterExpression() const;

  // ___________________________________________________________________________
  using Estimates = SparqlExpressionPimpl::Estimates;
  virtual Estimates getEstimatesForFilterExpression(
      uint64_t inputSizeEstimate,
      [[maybe_unused]] const std::optional<Variable>& primarySortKeyVariable)
      const;

  // Returns true iff this expression is a simple constant. Default
  // implementation returns `false`.
  virtual bool isConstantExpression() const;

  // Returns true iff this expression is a STR(...) expression.  Default
  // implementation returns `false`.
  virtual bool isStrExpression() const;

  // __________________________________________________________________________
  virtual ~SparqlExpression() = default;

  // Returns all the children of this expression. Typically only used for
  // testing
  virtual std::span<const SparqlExpression::Ptr> childrenForTesting()
      const final;

  virtual std::vector<SparqlExpression::Ptr> moveChildrenOut() && final;

  // Get the direct child expressions.
  virtual std::span<SparqlExpression::Ptr> children() final;
  virtual std::span<const SparqlExpression::Ptr> children() const final;

  // Return true if this expression or any of its ancestors in the expression
  // tree is an aggregate. For an example usage see the `LiteralExpression`
  // class.
  bool isInsideAggregate() const;

 private:
  virtual std::span<SparqlExpression::Ptr> childrenImpl() = 0;

  // Helper function for strings(). Get all variables, iris, and string literals
  // that are included in this expression directly, ignoring possible child
  // expressions.
  virtual std::span<const Variable> getContainedVariablesNonRecursive() const;

 protected:
  // After calling this function, `isInsideAlias()` (see below) returns true for
  // this expression as well as for all its descendants. This function must be
  // called by all child classes that are aggregate expressions.
  virtual void setIsInsideAggregate() final;
};
}  // namespace sparqlExpression
