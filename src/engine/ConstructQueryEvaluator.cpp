#include "ConstructQueryEvaluator.h"

#include "ExportQueryExecutionTrees.h"

std::optional<std::string> ConstructQueryEvaluator::evaluate(const Iri& iri) {
  return iri.iri();
}

std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const Literal& literal, PositionInTriple role) {
  if (role == PositionInTriple::OBJECT) {
    return literal.literal();
  }
  return std::nullopt;
}

std::optional<std::string> ConstructQueryEvaluator::evaluateVar(
    const Variable& var, const ConstructQueryExportContext& context) {
  size_t resultTableRow = context._resultTableRowIdx;
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
  stream << context._rowOffset + context._resultTableRowIdx << '_';
  stream << node.label();
  return stream.str();
}

std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const GraphTerm& term, const ConstructQueryExportContext& context,
    PositionInTriple posInTriple) {
  if (std::holds_alternative<Variable>(term)) {
    const Variable& var = std::get<Variable>(term);
    return ConstructQueryEvaluator::evaluateVar(var, context);
  }

  if (std::holds_alternative<BlankNode>(term)) {
    BlankNode node = std::get<BlankNode>(term);
    return ConstructQueryEvaluator::evaluate(node, context);
  }

  if (std::holds_alternative<Iri>(term)) {
    Iri iri = std::get<Iri>(term);
    return ConstructQueryEvaluator::evaluate(iri);
  }

  if (std::holds_alternative<Literal>(term)) {
    Literal literal = std::get<Literal>(term);
    return ConstructQueryEvaluator::evaluate(literal, posInTriple);
  }

  AD_FAIL();
}
