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
  static std::optional<std::string> evaluate(const Iri& iri);

  static std::optional<std::string> evaluate(const Literal& literal,
                                             PositionInTriple role);

  static std::optional<std::string> evaluate(
      const BlankNode& node, const ConstructQueryExportContext& context);

  static std::optional<std::string> evaluate(
      const Variable& var, const ConstructQueryExportContext& context);

  static std::optional<std::string> evaluate(
      const GraphTerm& term, const ConstructQueryExportContext& context,
      PositionInTriple posInTriple);

  // Evaluates a single CONSTRUCT triple pattern using the provided context.
  static StringTriple evaluateTriple(
      const std::array<GraphTerm, 3>& triple,
      const ConstructQueryExportContext& context);
};

#endif  // QLEVER_CONSTRUCTQUERYEVALUATOR_H
