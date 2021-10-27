//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "SparqlExpressionPimpl.h"

#include "SparqlExpression.h"

namespace sparqlExpression {

// __________________________________________________________________________
SparqlExpressionPimpl::SparqlExpressionPimpl(
    std::shared_ptr<SparqlExpression>&& pimpl)
    : _pimpl{std::move(pimpl)} {};

// ___________________________________________________________________________
SparqlExpressionPimpl::~SparqlExpressionPimpl() = default;

// ___________________________________________________________________________
std::vector<std::string*> SparqlExpressionPimpl::strings() {
  return _pimpl->strings();
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
std::optional<std::string>
SparqlExpressionPimpl::getVariableForNonDistinctCountOrNullopt() const {
  return _pimpl->getVariableForNonDistinctCountOrNullopt();
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
}  // namespace sparqlExpression
