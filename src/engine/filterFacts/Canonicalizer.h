// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_FILTERFACTS_CANONICALIZER_H
#define QLEVER_SRC_ENGINE_FILTERFACTS_CANONICALIZER_H

#include <variant>
#include <vector>

#include "backports/three_way_comparison.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "global/ValueIdComparators.h"
#include "parser/TripleComponent.h"

// The recognition of facts in FILTERs. A FILTER can be written in many
// equivalent ways, for example `geof:distance(?a, ?b, unit:M) <= 500`,
// `geof:distance(?a, ?b) <= 0.5`, or as one of several conditions combined
// with `&&`. A rewrite of the query (e.g. to a `SpatialJoin`) should not have
// to match each of them. Instead, each FILTER is brought into a canonical form
// (`canonicalize`, this file), in which typed facts are recognized (see
// `Recognizers.h`). The rewrites only use these facts.
//
// NOTE: The canonical form only models the shapes that the recognizers need.
// Every other expression stays `Opaque`, so it is never rewritten.
namespace filterFacts {

// `geof:distance(a_, b_)` in meters. `a_` and `b_` are variables or constant
// geometries.
struct Distance {
  TripleComponent a_;
  TripleComponent b_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(Distance, a_, b_)
};

// The comparison `lhs_ op_ rhs_` of a distance with a number `rhs_` (in
// meters), where `op_` is `<` or `<=`.
struct Constraint {
  Distance lhs_;
  valueIdComparators::Comparison op_;
  double rhs_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(Constraint, lhs_, op_, rhs_)
};

// Any other expression.
struct Opaque {
  const sparqlExpression::SparqlExpression* expression_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(Opaque, expression_)
};

// A conjunct of a FILTER (the FILTER holds iff all its conjuncts hold).
using Conjunct = std::variant<Constraint, Opaque>;

// Bring the expression of a FILTER into canonical form: split it at `&&` into
// its conjuncts, and bring each comparison `distance < number` or
// `distance <= number` into a `Constraint` (with the number in meters).
//
// NOTE: The result points into `filter` (for the `Opaque` conjuncts), so
// `filter` must outlive it.
std::vector<Conjunct> canonicalize(
    const sparqlExpression::SparqlExpression& filter);

}  // namespace filterFacts

#endif  // QLEVER_SRC_ENGINE_FILTERFACTS_CANONICALIZER_H
