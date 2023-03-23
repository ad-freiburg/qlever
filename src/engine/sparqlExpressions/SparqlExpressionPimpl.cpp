//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./SparqlExpressionPimpl.h"

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {

// __________________________________________________________________________
SparqlExpressionPimpl::SparqlExpressionPimpl(
    std::shared_ptr<SparqlExpression>&& pimpl, std::string descriptor)
    : _pimpl{std::move(pimpl)} {
  _pimpl->descriptor() = std::move(descriptor);
};

// ___________________________________________________________________________
SparqlExpressionPimpl::~SparqlExpressionPimpl() = default;

// ___________________________________________________________________________
std::vector<const Variable*> SparqlExpressionPimpl::containedVariables() const {
  return _pimpl->containedVariables();
}

// ____________________________________________________________________________
SparqlExpressionPimpl::SparqlExpressionPimpl(SparqlExpressionPimpl&&) noexcept =
    default;
SparqlExpressionPimpl& SparqlExpressionPimpl::operator=(
    SparqlExpressionPimpl&&) noexcept = default;
SparqlExpressionPimpl::SparqlExpressionPimpl(const SparqlExpressionPimpl&) =
    default;
SparqlExpressionPimpl& SparqlExpressionPimpl::operator=(
    const SparqlExpressionPimpl&) = default;

// ____________________________________________________________________________
std::vector<Variable> SparqlExpressionPimpl::getUnaggregatedVariables(
    const ad_utility::HashSet<Variable>& groupedVariables) const {
  auto vars = _pimpl->getUnaggregatedVariables();
  std::erase_if(
      vars, [&](const auto& var) { return groupedVariables.contains(var); });
  return vars;
}

// ___________________________________________________________________________
std::optional<SparqlExpressionPimpl::VariableAndDistinctness>
SparqlExpressionPimpl::getVariableForCount() const {
  return _pimpl->getVariableForCount();
}

// ___________________________________________________________________________
std::optional<::Variable> SparqlExpressionPimpl::getVariableOrNullopt() const {
  return _pimpl->getVariableOrNullopt();
}

// ___________________________________________________________________________
std::string SparqlExpressionPimpl::getCacheKey(
    const VariableToColumnMap& variableToColumnMap) const {
  return _pimpl->getCacheKey(variableToColumnMap);
}

// ____________________________________________________________________________
const std::string& SparqlExpressionPimpl::getDescriptor() const {
  return _pimpl->descriptor();
}

// ____________________________________________________________________________
void SparqlExpressionPimpl::setDescriptor(std::string descriptor) {
  _pimpl->descriptor() = std::move(descriptor);
}

// _____________________________________________________________________________
bool SparqlExpressionPimpl::isVariableContained(
    const Variable& variable) const {
  return std::ranges::any_of(
      containedVariables(),
      [&variable](const auto* varPtr) { return *varPtr == variable; });
}

// _____________________________________________________________________________
std::optional<SparqlExpressionPimpl::LangFilterData>
SparqlExpressionPimpl::getLanguageFilterExpression() const {
  return _pimpl->getLanguageFilterExpression();
}

// _____________________________________________________________________________
auto SparqlExpressionPimpl::getEstimatesForFilterExpression(
    uint64_t inputSizeEstimate,
    const std::optional<Variable>& primarySortKeyVariable) -> Estimates {
  return _pimpl->getEstimatesForFilterExpression(inputSizeEstimate,
                                                 primarySortKeyVariable);
}

// _____________________________________________________________________________
bool SparqlExpressionPimpl::containsLangExpression() const {
  return _pimpl->containsLangExpression();
}

// _____________________________________________________________________________
bool SparqlExpressionPimpl::containsAggregate() const {
  return _pimpl->containsAggregate();
}

// ______________________________________________________________________________
SparqlExpressionPimpl SparqlExpressionPimpl::makeVariableExpression(
    const Variable& variable) {
  return {std::make_unique<VariableExpression>(variable), variable.name()};
}
}  // namespace sparqlExpression
