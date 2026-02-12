// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructQueryEvaluator.h"

#include "engine/ExportQueryExecutionTrees.h"
#include "util/HashMap.h"
#include "util/TypeTraits.h"

// --- Methods operating on raw SPARQL types ---

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
std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const Variable& var, const ConstructQueryExportContext& context) {
  const auto& variableColumns = context._variableColumns;
  if (auto opt = ad_utility::getOptionalFromHashMap(variableColumns, var)) {
    return evaluateVariableByColumnIndex(opt->columnIndex_, context);
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

// _____________________________________________________________________________
ConstructQueryEvaluator::StringTriple ConstructQueryEvaluator::evaluateTriple(
    const std::array<GraphTerm, 3>& triple,
    const ConstructQueryExportContext& context) {
  using enum PositionInTriple;

  auto subject = evaluateTerm(triple[0], context, SUBJECT);
  auto predicate = evaluateTerm(triple[1], context, PREDICATE);
  auto object = evaluateTerm(triple[2], context, OBJECT);

  if (!subject.has_value() || !predicate.has_value() || !object.has_value()) {
    return StringTriple();
  }

  return StringTriple(std::move(subject.value()), std::move(predicate.value()),
                      std::move(object.value()));
}

// --- Methods operating on preprocessed types ---

// _____________________________________________________________________________
std::string ConstructQueryEvaluator::evaluatePreprocessed(
    const PrecomputedConstant& constant) {
  return constant.value_;
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluatePreprocessed(
    const PrecomputedVariable& variable,
    const ConstructQueryExportContext& context) {
  return evaluateVariableByColumnIndex(variable.columnIndex_, context);
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluatePreprocessed(
    const PrecomputedBlankNode& node,
    const ConstructQueryExportContext& context) {
  return absl::StrCat(node.prefix_,
                      context._rowOffset + context.resultTableRowIndex_,
                      node.suffix_);
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::evaluatePreprocessedTerm(
    const PreprocessedTerm& term, const ConstructQueryExportContext& context) {
  return std::visit(
      [&context](const auto& arg) -> std::optional<std::string> {
        using T = std::decay_t<decltype(arg)>;

        if constexpr (std::is_same_v<T, PrecomputedVariable>) {
          return evaluatePreprocessed(arg, context);
        } else if constexpr (std::is_same_v<T, PrecomputedBlankNode>) {
          return evaluatePreprocessed(arg, context);
        } else if constexpr (std::is_same_v<T, PrecomputedConstant>) {
          return evaluatePreprocessed(arg);
        } else {
          static_assert(ad_utility::alwaysFalse<T>);
        }
      },
      term);
}

// _____________________________________________________________________________
ConstructQueryEvaluator::StringTriple
ConstructQueryEvaluator::evaluatePreprocessedTriple(
    const PreprocessedTriple& triple,
    const ConstructQueryExportContext& context) {
  auto subject = evaluatePreprocessedTerm(triple[0], context);
  auto predicate = evaluatePreprocessedTerm(triple[1], context);
  auto object = evaluatePreprocessedTerm(triple[2], context);

  if (!subject.has_value() || !predicate.has_value() || !object.has_value()) {
    return StringTriple();
  }

  return StringTriple(std::move(subject.value()), std::move(predicate.value()),
                      std::move(object.value()));
}

// --- Core evaluation helpers ---

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
