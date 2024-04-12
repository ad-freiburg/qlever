//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {

// _____________________________________________________________________________
std::vector<const Variable*> SparqlExpression::containedVariables() const {
  std::vector<const Variable*> result;
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

// _____________________________________________________________________________
std::vector<Variable> SparqlExpression::getUnaggregatedVariables() {
  // Default implementation: This expression adds no variables, but all
  // unaggregated variables from the children remain unaggregated.
  std::vector<Variable> result;
  for (const auto& child : children()) {
    auto childResult = child->getUnaggregatedVariables();
    result.insert(result.end(), std::make_move_iterator(childResult.begin()),
                  std::make_move_iterator(childResult.end()));
  }
  return result;
}

// _____________________________________________________________________________
bool SparqlExpression::containsAggregate() const {
  if (isAggregate()) return true;
  return std::ranges::any_of(
      children(), [](const Ptr& child) { return child->containsAggregate(); });
}

// _____________________________________________________________________________
bool SparqlExpression::isAggregate() const { return false; }

// _____________________________________________________________________________
bool SparqlExpression::isDistinct() const {
  AD_THROW(
      "isDistinct() maybe only called for aggregate expressions. If this is "
      "an aggregate expression, then the implementation of isDistinct() is "
      "missing for this expression. Please report this to the developers.");
}

// _____________________________________________________________________________
void SparqlExpression::replaceChild(
    size_t childIndex, std::unique_ptr<SparqlExpression> newExpression) {
  AD_CONTRACT_CHECK(childIndex < children().size());
  children()[childIndex] = std::move(newExpression);
}

// _____________________________________________________________________________
const string& SparqlExpression::descriptor() const { return _descriptor; }

// _____________________________________________________________________________
string& SparqlExpression::descriptor() { return _descriptor; }

// _____________________________________________________________________________
std::optional<SparqlExpressionPimpl::VariableAndDistinctness>
SparqlExpression::getVariableForCount() const {
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<::Variable> SparqlExpression::getVariableOrNullopt() const {
  return std::nullopt;
}

// _____________________________________________________________________________
bool SparqlExpression::containsLangExpression() const {
  return std::ranges::any_of(children(),
                             [](const SparqlExpression::Ptr& child) {
                               return child->containsLangExpression();
                             });
}

// _____________________________________________________________________________
using LangFilterData = SparqlExpressionPimpl::LangFilterData;
std::optional<LangFilterData> SparqlExpression::getLanguageFilterExpression()
    const {
  return std::nullopt;
}

// _____________________________________________________________________________
using Estimates = SparqlExpressionPimpl::Estimates;
Estimates SparqlExpression::getEstimatesForFilterExpression(
    uint64_t inputSizeEstimate,
    [[maybe_unused]] const std::optional<Variable>& primarySortKeyVariable)
    const {
  // Default estimates: Each element can be computed in O(1) and nothing is
  // filtered out.
  return {inputSizeEstimate, inputSizeEstimate};
}

// _____________________________________________________________________________
bool SparqlExpression::isConstantExpression() const { return false; }

// _____________________________________________________________________________
bool SparqlExpression::isStrExpression() const { return false; }

// _____________________________________________________________________________
std::span<const SparqlExpression::Ptr> SparqlExpression::childrenForTesting()
    const {
  return children();
}

// _____________________________________________________________________________
std::vector<SparqlExpression::Ptr> SparqlExpression::moveChildrenOut() && {
  auto span = children();
  return {std::make_move_iterator(span.begin()),
          std::make_move_iterator(span.end())};
}

// _____________________________________________________________________________
std::span<SparqlExpression::Ptr> SparqlExpression::children() {
  return childrenImpl();
}

// _____________________________________________________________________________
std::span<const SparqlExpression::Ptr> SparqlExpression::children() const {
  auto children = const_cast<SparqlExpression&>(*this).children();
  return {children.data(), children.size()};
}

// _____________________________________________________________________________
std::span<const Variable> SparqlExpression::getContainedVariablesNonRecursive()
    const {
  // Default implementation: This expression adds no strings or variables.
  return {};
}

// _____________________________________________________________________________
void SparqlExpression::setIsInsideAggregate() {
  isInsideAggregate_ = true;
  // Note: `child` is a `unique_ptr` to a non-const object. So we could
  // technically use `const auto&` in the following loop, but this would be
  // misleading (the pointer is used in a `const` manner, but the pointee is
  // not).
  for (auto& child : children()) {
    child->setIsInsideAggregate();
  }
}

// _____________________________________________________________________________
bool SparqlExpression::isInsideAggregate() const { return isInsideAggregate_; }
}  // namespace sparqlExpression
