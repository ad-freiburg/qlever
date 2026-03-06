// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H

#include <memory>
#include <string>
#include <vector>

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTypes.h"
#include "util/http/MediaTypes.h"

namespace qlever::constructExport {

// Instantiates a single preprocessed term for a specific row.
// For constants: returns the precomputed string.
// For variables: looks up the batch-evaluated value.
// For blank nodes: computes the value on the fly using precomputed
//   prefix/suffix and the blank node row id (rowOffset + actualRowIdx).
std::optional<EvaluatedTerm> instantiateTerm(
    const PreprocessedTerm& term, const BatchEvaluationResult& batchResult,
    size_t rowInBatch, size_t blankNodeRowId);

// Instantiates all template triples for all rows in a batch. For each row,
// every triple in `tmpl.preprocessedTriples_` is instantiated; triples with
// any unbound term are silently dropped. `blankNodeBaseId` is the absolute
// row ID of the first row in the batch (used to generate unique blank node
// IDs).
std::vector<EvaluatedTriple> instantiateBatch(
    const PreprocessedConstructTemplate& tmpl,
    const BatchEvaluationResult& batchResult, size_t blankNodeBaseId);

// Render a single term to its string form.
// `shortForm=true`  (turtle, csv, tsv, string-triples): integers, decimals
//   and booleans are emitted without quotes or datatype annotation.
// `shortForm=false` (N-Triples): all typed literals carry an explicit
//   `"..."^^<type>` annotation.
// Terms with `type == nullptr` (IRIs, blank nodes, vocab-indexed literals)
// are returned as-is regardless of `shortForm`.
std::string renderTerm(const EvaluatedTermData& term, bool shortForm);

// Formats a triple (subject, predicate, object) according to the output
// format.
std::string formatTriple(const EvaluatedTerm& subject,
                         const EvaluatedTerm& predicate,
                         const EvaluatedTerm& object,
                         const ad_utility::MediaType& format);

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
