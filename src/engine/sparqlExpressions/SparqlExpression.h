//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SPARQLEXPRESSION_H
#define QLEVER_SPARQLEXPRESSION_H

#include <memory>
#include <span>
#include <variant>
#include <vector>

#include "engine/CallFixedSize.h"
#include "engine/QueryExecutionContext.h"
#include "engine/ResultTable.h"
#include "engine/sparqlExpressions/SetOfIntervals.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/Id.h"
#include "parser/data/Variable.h"
#include "util/ConstexprSmallString.h"

namespace sparqlExpression {

// TODO<joka921>  Move the definitions of the functions into a
// `SparqlExpression.cpp`

/// Virtual base class for an arbitrary Sparql Expression which holds the
/// structure of the expression as well as the logic to evaluate this expression
/// on a given intermediate result
class SparqlExpression {
 private:
  std::string _descriptor;
  bool isInsideAggregate_ = false;

 public:
  /// ________________________________________________________________________
  using Ptr = std::unique_ptr<SparqlExpression>;

  /// Evaluate a Sparql expression.
  virtual ExpressionResult evaluate(EvaluationContext*) const = 0;

  /// Return all variables and IRIs, needed for certain parser methods.
  /// TODO<joka921> should be called getStringLiteralsAndVariables
  virtual vector<const Variable*> containedVariables() const final {
    vector<const Variable*> result;
    // Recursively aggregate the strings from all children.
    for (const auto& child : children()) {
      auto variablesFromChild = child->containedVariables();
      result.insert(result.end(), variablesFromChild.begin(),
                    variablesFromChild.end());
    }

    // Add the strings from this expression.
    auto locallyAdded = getContainedVariablesNonRecursive();
    for (auto& el : locallyAdded) {
      result.push_back(&el);
    }
    return result;
  }

  /// Return all the variables that occur in the expression, but are not
  /// aggregated.
  virtual vector<std::string> getUnaggregatedVariables() {
    // Default implementation: This expression adds no variables, but all
    // unaggregated variables from the children remain unaggregated.
    std::vector<string> result;
    for (const auto& child : children()) {
      auto childResult = child->getUnaggregatedVariables();
      result.insert(result.end(), std::make_move_iterator(childResult.begin()),
                    std::make_move_iterator(childResult.end()));
    }
    return result;
  }

  // Return true iff this expression contains an aggregate like SUM, COUNT etc.
  // This information is needed to check if there is an implicit GROUP BY in a
  // query because any of the selected aliases contains an aggregate.
  virtual bool containsAggregate() const {
    return std::ranges::any_of(children(), [](const Ptr& child) {
      return child->containsAggregate();
    });
  }

  /// Get a unique identifier for this expression, used as cache key.
  virtual string getCacheKey(const VariableToColumnMap& varColMap) const = 0;

  /// Get a short, human readable identifier for this expression.
  virtual const string& descriptor() const final { return _descriptor; }
  virtual string& descriptor() final { return _descriptor; }

  /// For the pattern trick we need to know, whether this expression
  /// is a non-distinct count of a single variable. In this case we return
  /// the variable. Otherwise we return std::nullopt.
  virtual std::optional<SparqlExpressionPimpl::VariableAndDistinctness>
  getVariableForCount() const {
    return std::nullopt;
  }

  /// Helper function for getVariableForCount() : If this
  /// expression is a single variable, return the name of this variable.
  /// Otherwise, return std::nullopt.
  virtual std::optional<::Variable> getVariableOrNullopt() const {
    return std::nullopt;
  }

  // For the following three functions (`containsLangExpression`,
  // `getLanguageFilterExpression`, and `getEstimatesForFilterExpression`, see
  // the documentation of the functions of the same names in
  // `SparqlExpressionPimpl.h`. Each of them has a default implementation that
  // is correct for most of the expressions.
  virtual bool containsLangExpression() const {
    return std::ranges::any_of(children(),
                               [](const SparqlExpression::Ptr& child) {
                                 return child->containsLangExpression();
                               });
  }

  // ___________________________________________________________________________
  using LangFilterData = SparqlExpressionPimpl::LangFilterData;
  virtual std::optional<LangFilterData> getLanguageFilterExpression() const {
    return std::nullopt;
  }

  // ___________________________________________________________________________
  using Estimates = SparqlExpressionPimpl::Estimates;
  virtual Estimates getEstimatesForFilterExpression(
      uint64_t inputSizeEstimate,
      [[maybe_unused]] const std::optional<Variable>& primarySortKeyVariable)
      const {
    // Default estimates: Each element can be computed in O(1) and nothing is
    // filtered out.
    return {inputSizeEstimate, inputSizeEstimate};
  }

  // Returns true iff this expression is a simple constant. Default
  // implementation returns `false`.
  virtual bool isConstantExpression() const { return false; }

  // __________________________________________________________________________
  virtual ~SparqlExpression() = default;

  // Returns all the children of this expression. Typically only used for
  // testing
  virtual std::span<const SparqlExpression::Ptr> childrenForTesting()
      const final {
    return children();
  }

 private:
  // Get the direct child expressions.
  virtual std::span<SparqlExpression::Ptr> children() = 0;
  virtual std::span<const SparqlExpression::Ptr> children() const final {
    auto children = const_cast<SparqlExpression&>(*this).children();
    return {children.data(), children.size()};
  }

  // Helper function for strings(). Get all variables, iris, and string literals
  // that are included in this expression directly, ignoring possible child
  // expressions.
  virtual std::span<const Variable> getContainedVariablesNonRecursive() const {
    // Default implementation: This expression adds no strings or variables.
    return {};
  }

 protected:
  // After calling this function, `isInsideAlias()` (see below) returns true for
  // this expression as well as for all its descendants. This function must be
  // called by all child classes that are aggregate expressions.
  virtual void setIsInsideAggregate() final {
    isInsideAggregate_ = true;
    // Note: `child` is a `unique_ptr` to a non-const object. So we could
    // technically use `const auto&` in the following loop, but this would be
    // misleading (the pointer is used in a `const` manner, but the pointee is
    // not).
    for (auto& child : children()) {
      child->setIsInsideAggregate();
    }
  }

  // Return true if this class or any of its ancestors in the expression tree is
  // an aggregate. For an example usage see the `LiteralExpression` class.
  bool isInsideAggregate() const { return isInsideAggregate_; }
};
}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSION_H
