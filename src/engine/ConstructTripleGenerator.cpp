// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures

#include "engine/ConstructTripleGenerator.h"

#include "engine/ConstructQueryEvaluator.h"

TripleEvaluator::StringTriple TripleEvaluator::operator()(
    const std::array<GraphTerm, 3>& triple) const {
  cancellationHandle_->throwIfCancelled();

  using enum PositionInTriple;

  auto subject =
      ConstructQueryEvaluator::evaluate(triple[0], context_, SUBJECT);
  auto predicate =
      ConstructQueryEvaluator::evaluate(triple[1], context_, PREDICATE);
  auto object = ConstructQueryEvaluator::evaluate(triple[2], context_, OBJECT);

  if (!subject.has_value() || !predicate.has_value() || !object.has_value()) {
    return StringTriple();  // Empty triple indicates UNDEF
  }

  return StringTriple(std::move(subject.value()), std::move(predicate.value()),
                      std::move(object.value()));
}
