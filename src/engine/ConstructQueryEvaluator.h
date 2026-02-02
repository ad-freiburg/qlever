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
  using StringTriple = QueryExecutionTree::StringTriple;

 public:
  // Helper method for `evaluateTerm`. Evaluates an `Iri` (which is part of a
  // CONSTRUCT triple pattern).
  static std::optional<std::string> evaluate(const Iri& iri);

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

  // Evaluates a `GraphTerm` (which is part of a CONSTRUCT triple pattern) using
  // the provided context and the position of the `GraphTerm` in the template
  // triple. If the `GraphTerm` can't be evaluated, `std::nullopt` is returned.
  static std::optional<std::string> evaluateTerm(
      const GraphTerm& term, const ConstructQueryExportContext& context,
      PositionInTriple posInTriple);

  // Evaluates a single CONSTRUCT triple pattern using the provided context. If
  // any of the `GraphTerm` elements can't be evaluated, an empty `StringTriple`
  // is returned. (meaning that all three member variables `subject_` ,
  // `predicate_`, `object_` of the `StringTriple` are set to the empty string).
  static StringTriple evaluateTriple(
      const std::array<GraphTerm, 3>& triple,
      const ConstructQueryExportContext& context);
};

#endif  // QLEVER_CONSTRUCTQUERYEVALUATOR_H
