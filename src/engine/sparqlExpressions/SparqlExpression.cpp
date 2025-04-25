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
std::vector<Variable> SparqlExpression::getUnaggregatedVariables() const {
  // Aggregates always aggregate over all variables, so no variables remain
  // unaggregated.
  if (isAggregate() != AggregateStatus::NoAggregate) {
    return {};
  }
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
  if (isAggregate() != AggregateStatus::NoAggregate) {
    AD_CORRECTNESS_CHECK(isInsideAggregate());
    return true;
  }

  return ql::ranges::any_of(
      children(), [](const Ptr& child) { return child->containsAggregate(); });
}

// _____________________________________________________________________________
auto SparqlExpression::isAggregate() const -> AggregateStatus {
  return AggregateStatus::NoAggregate;
}

// _____________________________________________________________________________
std::unique_ptr<SparqlExpression> SparqlExpression::replaceChild(
    size_t childIndex, std::unique_ptr<SparqlExpression> newExpression) {
  AD_CONTRACT_CHECK(childIndex < children().size());
  return std::exchange(children()[childIndex], std::move(newExpression));
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
  return ql::ranges::any_of(children(), [](const SparqlExpression::Ptr& child) {
    return child->containsLangExpression();
  });
}

// _____________________________________________________________________________
bool SparqlExpression::isYearExpression() const { return false; }

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
// The default implementation returns an empty vector given that for most
// `SparqlExpressions` no pre-filter procedure is available. Only specific
// expressions that yield boolean values support the construction of
// <`PrefilterExpression`, `Variable`> pairs. For more information, refer to the
// declaration of this method in SparqlExpression.h. `SparqlExpression`s for
// which pre-filtering over the `IndexScan` is supported, override the virtual
// `getPrefilterExpressionForMetadata` method declared there.
std::vector<PrefilterExprVariablePair>
SparqlExpression::getPrefilterExpressionForMetadata(
    [[maybe_unused]] bool isNegated) const {
  return {};
};

// _____________________________________________________________________________
bool SparqlExpression::isConstantExpression() const { return false; }

// _____________________________________________________________________________
bool SparqlExpression::isStrExpression() const { return false; }

// _____________________________________________________________________________
ql::span<const SparqlExpression::Ptr> SparqlExpression::childrenForTesting()
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
ql::span<SparqlExpression::Ptr> SparqlExpression::children() {
  return childrenImpl();
}

// _____________________________________________________________________________
ql::span<const SparqlExpression::Ptr> SparqlExpression::children() const {
  auto children = const_cast<SparqlExpression&>(*this).children();
  return {children.data(), children.size()};
}

// _____________________________________________________________________________
ql::span<const Variable> SparqlExpression::getContainedVariablesNonRecursive()
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
bool SparqlExpression::isInsideAggregate() const {
  if (isAggregate() != AggregateStatus::NoAggregate) {
    AD_CORRECTNESS_CHECK(
        isInsideAggregate_,
        "This indicates a missing call to `setIsInsideAggregate()` inside the "
        "constructor of an aggregate expression");
  }
  return isInsideAggregate_;
}

// ________________________________________________________________
bool SparqlExpression::isExistsExpression() const { return false; }

//______________________________________________________________________________
template <typename SparqlExpressionT>
void getExistsExpressionsImpl(SparqlExpressionT& self,
                              std::vector<SparqlExpressionT*>& result) {
  static_assert(ad_utility::isSimilar<SparqlExpressionT, SparqlExpression>);
  if (self.isExistsExpression()) {
    result.push_back(&self);
  }
  for (auto& child : self.children()) {
    child->getExistsExpressions(result);
  }
}

//______________________________________________________________________________
void SparqlExpression::getExistsExpressions(
    std::vector<const SparqlExpression*>& result) const {
  getExistsExpressionsImpl(*this, result);
}
//______________________________________________________________________________
void SparqlExpression::getExistsExpressions(
    std::vector<SparqlExpression*>& result) {
  getExistsExpressionsImpl(*this, result);
}
}  // namespace sparqlExpression
