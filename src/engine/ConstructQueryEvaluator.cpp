// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructQueryEvaluator.h"

#include "engine/ExportQueryExecutionTrees.h"

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
std::optional<std::string> ConstructQueryEvaluator::evaluate(
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

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const BlankNode& node, const ConstructQueryExportContext& context) {
  std::ostringstream stream;
  stream << "_:";
  stream << (node.isGenerated() ? 'g' : 'u');  // generated or user-defined
  stream << context._rowOffset + context._resultTableRowIdx << '_';
  stream << node.label();
  return stream.str();
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const GraphTerm& term, const ConstructQueryExportContext& context,
    PositionInTriple posInTriple) {
  return std::visit(
      [&context, &posInTriple](auto&& arg) -> std::optional<std::string> {
        // strips reference/const qualifiers
        using T = std::decay_t<decltype(arg)>;

        if constexpr (std::is_same_v<T, Variable>) {
          return ConstructQueryEvaluator::evaluate(arg, context);
        } else if constexpr (std::is_same_v<T, BlankNode>) {
          return ConstructQueryEvaluator::evaluate(arg, context);
        } else if constexpr (std::is_same_v<T, Iri>) {
          return ConstructQueryEvaluator::evaluate(arg);
        } else if constexpr (std::is_same_v<T, Literal>) {
          return ConstructQueryEvaluator::evaluate(arg, posInTriple);
        } else {
          AD_FAIL();
        }
      },
      term);
}

// _____________________________________________________________________________
ConstructQueryEvaluator::StringTriple ConstructQueryEvaluator::evaluateTriple(
    const std::array<GraphTerm, 3>& triple,
    const ConstructQueryExportContext& context) {
  // We specify the position to the evaluator so it knows how to handle
  // special cases (like blank node generation or IRI escaping).
  using enum PositionInTriple;

  auto subject = ConstructQueryEvaluator::evaluate(triple[0], context, SUBJECT);
  auto predicate =
      ConstructQueryEvaluator::evaluate(triple[1], context, PREDICATE);
  auto object = ConstructQueryEvaluator::evaluate(triple[2], context, OBJECT);

  // In SPARQL CONSTRUCT, if any part of the triple (S, P, or O) evaluates
  // to UNDEF, the entire triple is omitted from the result.
  if (!subject.has_value() || !predicate.has_value() || !object.has_value()) {
    return StringTriple();  // Returns an empty triple which is filtered out
    // later
  }

  return StringTriple(std::move(subject.value()), std::move(predicate.value()),
                      std::move(object.value()));
}
