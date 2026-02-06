// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_CONSTRUCTQUERYEVALUATOR_H
#define QLEVER_CONSTRUCTQUERYEVALUATOR_H

#include "engine/QueryExecutionTree.h"
#include "parser/data/BlankNode.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "parser/data/GraphTerm.h"
#include "parser/data/Iri.h"
#include "parser/data/Literal.h"

class ConstructQueryEvaluator {
 public:
  // Helper method for `evaluateTerm`. Evaluates an `Iri` (which is part of a
  // CONSTRUCT triple pattern).
  static std::string evaluate(const Iri& iri);

  // Helper method for `evaluateTerm`. Evaluates a `Literal` (which is part of
  // a CONSTRUCT triple pattern) using the position of the literal in the
  // template triple (literals are only allowed to be in the OBJECT position
  // of a triple).
  static std::optional<std::string> evaluate(const Literal& literal,
                                             PositionInTriple role);

  // Helper method for `evaluateTerm`. Evaluates a `BlankNode` (which is part of
  // a CONSTRUCT triple pattern) using the provided context.
  static std::optional<std::string> evaluate(
      const BlankNode& node, const ConstructQueryExportContext& context);

  // Helper method for `evaluateTerm`. Evaluates a `Variable` (which is part of
  // a CONSTRUCT triple pattern) using the provided context.
  static std::optional<std::string> evaluate(
      const Variable& var, const ConstructQueryExportContext& context);

  // Evaluates a `Variable` on the given `ConstructQueryExportContext`.
  // The `Variable` is given implicitly by the `columnIndex`, that is, the idx
  // of the column in the `IdTable` which the `Variable` is uniquely identified
  // by. The necessary context for doing said evaluation is contained in
  // `ConstructQueryExportContext`, such as the idx specifying the row of the
  // result table which this variable should be evaluated for and the vocabulary
  // for looking up the actual string value that the `Id`, which the variable is
  // mapped to for the given result table row, resolves to.
  static std::optional<std::string> evaluateVariableByColumnIndex(
      std::optional<size_t> columnIndex,
      const ConstructQueryExportContext& context);

  // Evaluates a `GraphTerm` (which is part of a CONSTRUCT triple pattern) using
  // the provided context and the position of the `GraphTerm` in the template
  // triple. If the `GraphTerm` can't be evaluated, `std::nullopt` is returned.
  static std::optional<std::string> evaluateTerm(
      const GraphTerm& term, const ConstructQueryExportContext& context,
      PositionInTriple posInTriple);
};

#endif  // QLEVER_CONSTRUCTQUERYEVALUATOR_H
