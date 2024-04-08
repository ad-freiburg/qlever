//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/SparqlExpressionTypes.h"

namespace sparqlExpression {

IdOrString::IdOrString(std::optional<std::string> s) {
  if (s.has_value()) {
    emplace<std::string>(std::move(s.value()));
  } else {
    emplace<Id>(Id::makeUndefined());
  }
}

// _____________________________________________________________________________
void PrintTo(const IdOrString& var, std::ostream* os) {
  std::visit([&os](const auto& s) { *os << s; }, var);
}

// _____________________________________________________________________________
EvaluationContext::EvaluationContext(
    const QueryExecutionContext& qec,
    const VariableToColumnMap& variableToColumnMap, const IdTable& inputTable,
    const ad_utility::AllocatorWithLimit<Id>& allocator,
    const LocalVocab& localVocab,
    ad_utility::SharedCancellationHandle cancellationHandle)
    : _qec{qec},
      _variableToColumnMap{variableToColumnMap},
      _inputTable{inputTable},
      _allocator{allocator},
      _localVocab{localVocab},
      cancellationHandle_{std::move(cancellationHandle)} {
  AD_CONTRACT_CHECK(cancellationHandle_);
}

// _____________________________________________________________________________
bool EvaluationContext::isResultSortedBy(const Variable& variable) {
  if (_columnsByWhichResultIsSorted.empty()) {
    return false;
  }
  if (!_variableToColumnMap.contains(variable)) {
    return false;
  }

  return getColumnIndexForVariable(variable) ==
         _columnsByWhichResultIsSorted[0];
}

// _____________________________________________________________________________
[[nodiscard]] size_t EvaluationContext::size() const {
  return _endIndex - _beginIndex;
}

// _____________________________________________________________________________
[[nodiscard]] ColumnIndex EvaluationContext::getColumnIndexForVariable(
    const Variable& var) const {
  const auto& map = _variableToColumnMap;
  if (!map.contains(var)) {
    throw std::runtime_error(absl::StrCat(
        "Variable ", var.name(), " was not found in input to expression."));
  }
  return map.at(var).columnIndex_;
}

// _____________________________________________________________________________
std::optional<ExpressionResult>
EvaluationContext::getResultFromPreviousAggregate(const Variable& var) const {
  const auto& map = _variableToColumnMapPreviousResults;
  if (!map.contains(var)) {
    return std::nullopt;
  }
  ColumnIndex idx = map.at(var).columnIndex_;
  AD_CONTRACT_CHECK(idx < _previousResultsFromSameGroup.size());

  return copyExpressionResult(_previousResultsFromSameGroup.at(idx));
}

}  // namespace sparqlExpression
