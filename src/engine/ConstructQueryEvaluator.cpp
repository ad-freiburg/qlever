#include "ConstructQueryEvaluator.h"

#include "ExportQueryExecutionTrees.h"

std::optional<std::string> ConstructQueryEvaluator::evaluate(const Iri& iri) {
  return iri.toStringRepresentation();
}

std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const Literal& literal, PositionInTriple role) {
  if (role == PositionInTriple::OBJECT) {
    return literal.toStringRepresentation();
  }
  return std::nullopt;
}

std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const Variable& var, const ConstructQueryExportContext& context) {
  size_t resultTableRow = context._resultTableRow;
  const auto& variableColumns = context._variableColumns;
  const Index& qecIndex = context._qecIndex;
  const IdTable& idTable = context.idTable_;

  if (variableColumns.contains(var)) {
    size_t index = variableColumns.at(var).columnIndex_;
    auto id = idTable(resultTableRow, index);
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
  return std::nullopt;
}

std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const BlankNode& node, const ConstructQueryExportContext& context) {
  std::ostringstream stream;
  stream << "_:";
  stream << (node.isGenerated() ? 'g' : 'u');  // generated or user-defined
  stream << context._rowOffset + context._resultTableRow << '_';
  stream << node.label();
  return stream.str();
}
