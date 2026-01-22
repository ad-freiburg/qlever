// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTripleGenerator.h"

#include "engine/ConstructQueryEvaluator.h"

// _____________________________________________________________________________
ConstructTripleGenerator::StringTriple ConstructTripleGenerator::evaluateTriple(
    const std::array<GraphTerm, 3>& triple,
    const ConstructQueryExportContext& context) const {
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
