//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "SparqlExpressionPimpl.h"

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {

// __________________________________________________________________________
SparqlExpressionPimpl::SparqlExpressionPimpl(
    std::shared_ptr<SparqlExpression>&& pimpl)
    : _pimpl{std::move(pimpl)} {};

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
std::vector<std::string> SparqlExpressionPimpl::getUnaggregatedVariables()
    const {
  return _pimpl->getUnaggregatedVariables();
}

// ___________________________________________________________________________
std::optional<::Variable>
SparqlExpressionPimpl::getVariableForNonDistinctCountOrNullopt() const {
  return _pimpl->getVariableForNonDistinctCountOrNullopt();
}

// ___________________________________________________________________________
std::optional<::Variable> SparqlExpressionPimpl::getVariableOrNullopt() const {
  return _pimpl->getVariableOrNullopt();
}

// ___________________________________________________________________________
std::string SparqlExpressionPimpl::getCacheKey(
    const VariableColumnMap& variableColumnMap) const {
  return _pimpl->getCacheKey(variableColumnMap);
}

// ____________________________________________________________________________
const std::string& SparqlExpressionPimpl::getDescriptor() const {
  return _pimpl->descriptor();
}

// ____________________________________________________________________________
void SparqlExpressionPimpl::setDescriptor(std::string descriptor) {
  _pimpl->descriptor() = std::move(descriptor);
}
}  // namespace sparqlExpression
