// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructQueryEvaluator.h"

#include "engine/ExportQueryExecutionTrees.h"
#include "util/HashMap.h"
#include "util/TypeTraits.h"

// _____________________________________________________________________________
std::string ConstructQueryEvaluator::evaluate(const Iri& iri) {
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
std::optional<std::string> ConstructQueryEvaluator::evaluateId(
    Id id, const Index& index, const LocalVocab& localVocab) {
  auto optionalStringAndType =
      ExportQueryExecutionTrees::idToStringAndType(index, id, localVocab);

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
std::optional<std::string>
ConstructQueryEvaluator::evaluateVariableByColumnIndex(
    std::optional<size_t> columnIndex,
    const ConstructQueryExportContext& context) {
  if (!columnIndex.has_value()) {
    return std::nullopt;
  }

  auto id = context.idTable_(context.resultTableRowIndex_, columnIndex.value());
  return evaluateId(id, context._qecIndex, context.localVocab_);
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const Variable& var, const ConstructQueryExportContext& context) {
  const auto& variableColumns = context._variableColumns;
  if (auto opt = ad_utility::getOptionalFromHashMap(variableColumns, var)) {
    return evaluateVariableByColumnIndex(opt->columnIndex_, context);
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluateTerm(
    const PreprocessedTerm& term, const ConstructQueryExportContext& context,
    PositionInTriple posInTriple) {
  return std::visit(
      [&context, &posInTriple](const auto& arg) -> std::optional<std::string> {
        // strips reference/const qualifiers
        using T = std::decay_t<decltype(arg)>;

        if constexpr (std::is_same_v<T, PrecomputedVariable>) {
          return evaluate(arg, context);
        } else if constexpr (std::is_same_v<T, PrecomputedBlankNode>) {
          return evaluate(arg, context);
        } else if constexpr (std::is_same_v<T, PrecomputedConstant>) {
          return evaluate(arg);
        } else {
          static_assert(ad_utility::alwaysFalse<T>);
        }
      },
      term);
}

// _____________________________________________________________________________
ConstructQueryEvaluator::StringTriple ConstructQueryEvaluator::evaluateTriple(
    const PreprocessedTriple& triple,
    const ConstructQueryExportContext& context) {
  // We specify the position to the evaluator so it knows how to handle
  // special cases (like blank node generation or IRI escaping).
  using enum PositionInTriple;

  auto subject = evaluateTerm(triple[0], context, SUBJECT);
  auto predicate = evaluateTerm(triple[1], context, PREDICATE);
  auto object = evaluateTerm(triple[2], context, OBJECT);

  // In SPARQL CONSTRUCT, if any part of the triple (S, P, or O) evaluates
  // to UNDEF, the entire triple is omitted from the result.
  if (!subject.has_value() || !predicate.has_value() || !object.has_value()) {
    return StringTriple();  // Returns an empty triple which is filtered out
    // later
  }

  return StringTriple(std::move(subject.value()), std::move(predicate.value()),
                      std::move(object.value()));
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const PrecomputedVariable& variable,
    const ConstructQueryExportContext& context) {
  std::optional<size_t> colIdx = variable.columnIndex_;
  if (colIdx.has_value()) {
    return evaluateVariableByColumnIndex(variable.columnIndex_, context);
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::string ConstructQueryEvaluator::evaluate(
    const PrecomputedConstant& constant) {
  return constant.value_;
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const PrecomputedBlankNode& node,
    const ConstructQueryExportContext& context) {
  return absl::StrCat(node.prefix_, context.resultTableRowIndex_, node.suffix_);
}
