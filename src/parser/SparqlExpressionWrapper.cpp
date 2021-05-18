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
}  // namespace sparqlExpression
