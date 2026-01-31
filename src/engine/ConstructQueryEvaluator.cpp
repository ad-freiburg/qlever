// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructQueryEvaluator.h"

#include "engine/ExportQueryExecutionTrees.h"
#include "util/TypeTraits.h"

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluate(const Iri& iri) {
  return iri.iri();
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const Literal& literal, PositionInTriple role) {
  if (role == PositionInTriple::OBJECT) {
    return literal.literal();
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluateWithColumnIndex(
    std::optional<size_t> columnIndex,
    const ConstructQueryExportContext& context) {
  if (!columnIndex.has_value()) {
    return std::nullopt;
  }

  size_t resultTableRow = context.resultTableRowIndex_;
  const Index& qecIndex = context._qecIndex;
  const IdTable& idTable = context.idTable_;

  auto id = idTable(resultTableRow, columnIndex.value());
  auto optionalStringAndType = ExportQueryExecutionTrees::idToStringAndType(
      qecIndex, id, context.localVocab_);

  if (!optionalStringAndType.has_value()) {
    return std::nullopt;
  }

  auto& [literal, type] = optionalStringAndType.value();
  const char* i = XSD_INT_TYPE;
  const char* d = XSD_DECIMAL_TYPE;
  const char* b = XSD_BOOLEAN_TYPE;

  // Note: If `type` is `XSD_DOUBLE_TYPE`, `literal` is always "NaN", "INF" or
  // "-INF", which doesn't have a short form notation.
  if (type == nullptr || type == i || type == d ||
      (type == b && literal.length() > 1)) {
    return std::move(literal);
  } else {
    return absl::StrCat("\"", literal, "\"^^<", type, ">");
  }
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const Variable& var, const ConstructQueryExportContext& context) {
  const auto& variableColumns = context._variableColumns;
  if (variableColumns.contains(var)) {
    return evaluateWithColumnIndex(variableColumns.at(var).columnIndex_,
                                   context);
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const BlankNode& node, const ConstructQueryExportContext& context) {
  // Use absl::StrCat for efficient string concatenation (single allocation)
  // instead of std::ostringstream which has significant overhead.
  // Note: absl::StrCat doesn't accept single chars, so we use string literals.
  return absl::StrCat("_:", (node.isGenerated() ? "g" : "u"),
                      context._rowOffset + context.resultTableRowIndex_, "_",
                      node.label());
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluateTerm(
    const GraphTerm& term, const ConstructQueryExportContext& context,
    PositionInTriple posInTriple) {
  return std::visit(
      [&context, &posInTriple](const auto& arg) -> std::optional<std::string> {
        // strips reference/const qualifiers
        using T = std::decay_t<decltype(arg)>;

        if constexpr (std::is_same_v<T, Variable>) {
          return evaluate(arg, context);
        } else if constexpr (std::is_same_v<T, BlankNode>) {
          return evaluate(arg, context);
        } else if constexpr (std::is_same_v<T, Iri>) {
          return evaluate(arg);
        } else if constexpr (std::is_same_v<T, Literal>) {
          return evaluate(arg, posInTriple);
        } else {
          static_assert(ad_utility::alwaysFalse<T>);
        }
      },
      term);
}
