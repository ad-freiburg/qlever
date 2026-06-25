// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <sebastian.walter98@gmail.com>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EMBEDDINGEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EMBEDDINGEXPRESSION_H

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {

// Factory for the `embf:distance(a, b, type)` SPARQL expression function.
// `child1`/`child2` are the two vectors `a`/`b` (each a stored or inline
// `emb:fp32Vector` literal); `child3` is the embedding-type IRI (the object of
// `emb:type`) and must be a constant IRI (else a parse-time error is thrown
// here). The result is a cosine / L2 / squared-L2 / dot-product *distance*
// (smaller = closer) per the type's `emb:hasMetric`. Type resolution and all
// per-row validation happen at evaluation time against the index's
// `EmbeddingTypeRegistry`.
SparqlExpression::Ptr makeEmbeddingDistanceExpression(
    SparqlExpression::Ptr child1, SparqlExpression::Ptr child2,
    SparqlExpression::Ptr child3);

}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EMBEDDINGEXPRESSION_H
