//
// Created by johannes on 13.05.21.
//

#include "SparqlExpressionPimpl.h"

#include "SparqlExpression.h"

namespace sparqlExpression {
SparqlExpressionPimpl::SparqlExpressionPimpl(
    std::shared_ptr<SparqlExpression>&& pimpl)
    : _pimpl{std::move(pimpl)} {};

SparqlExpressionPimpl::~SparqlExpressionPimpl() = default;
std::vector<std::string*> SparqlExpressionPimpl::strings() {
  return _pimpl->strings();
}
SparqlExpressionPimpl::SparqlExpressionPimpl(SparqlExpressionPimpl&&) noexcept =
    default;
SparqlExpressionPimpl& SparqlExpressionPimpl::operator=(
    SparqlExpressionPimpl&&) noexcept = default;
SparqlExpressionPimpl::SparqlExpressionPimpl(const SparqlExpressionPimpl&) =
    default;
SparqlExpressionPimpl& SparqlExpressionPimpl::operator=(
    const SparqlExpressionPimpl&) = default;

std::vector<std::string> SparqlExpressionPimpl::getUnaggregatedVariables()
    const {
  return _pimpl->getUnaggregatedVariables();
}

std::optional<std::string>
SparqlExpressionPimpl::isNonDistinctCountOfSingleVariable() const {
  return _pimpl->getVariableForNonDistinctCountOrNullopt();
}

std::string SparqlExpressionPimpl::asString(
    const VariableColumnMap& variableColumnMap) const {
  return _pimpl->getCacheKey(variableColumnMap);
}

// ____________________________________________________________________________
const std::string& SparqlExpressionPimpl::getDescriptor() const {
  return _pimpl->descriptor();
}
}  // namespace sparqlExpression
