//
// Created by johannes on 13.05.21.
//

#include "SparqlExpressionWrapper.h"
#include "SparqlExpression.h"

namespace sparqlExpression {
SparqlExpressionWrapper::SparqlExpressionWrapper(
    std::shared_ptr<SparqlExpression>&& pimpl)
    : _pimpl{std::move(pimpl)} {};

SparqlExpressionWrapper::~SparqlExpressionWrapper() = default;
std::vector<std::string*> SparqlExpressionWrapper::strings() {
  return _pimpl->strings();
}
SparqlExpressionWrapper::SparqlExpressionWrapper(
    SparqlExpressionWrapper&&) noexcept = default;
SparqlExpressionWrapper& SparqlExpressionWrapper::operator=(
    SparqlExpressionWrapper&&) noexcept = default;
SparqlExpressionWrapper::SparqlExpressionWrapper(
    const SparqlExpressionWrapper&) = default;
SparqlExpressionWrapper& SparqlExpressionWrapper::operator=(
    const SparqlExpressionWrapper&) = default;

std::vector<std::string> SparqlExpressionWrapper::getUnaggregatedVariables()
    const {
  return _pimpl->getUnaggregatedVariables();
}

std::optional<std::string>
SparqlExpressionWrapper::isNonDistinctCountOfSingleVariable() const {
  // TODO<joka921> pass this down to enable pattern trick!!
  return std::nullopt;
}

std::string SparqlExpressionWrapper::asString(
    const VariableColumnMap& variableColumnMap) const {
  return _pimpl->getCacheKey(variableColumnMap);
}
}  // namespace sparqlExpression
