// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H

#include <functional>
#include <string>
#include <vector>

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTypes.h"
#include "engine/QueryExecutionTree.h"
#include "util/Iterators.h"
#include "util/http/MediaTypes.h"

namespace qlever::constructExport {

using StringTriple = QueryExecutionTree::StringTriple;

class ConstructDeduplicator;

// Instantiates a single preprocessed term for a specific row.
// For constants: returns the precomputed string.
// For variables: looks up the batch-evaluated value.
// For blank nodes: computes the value on the fly using precomputed
//   prefix/suffix and the blank node row id (rowOffset + actualRowIdx).
std::optional<EvaluatedTerm> instantiateTerm(
    const PreprocessedTerm& term, const BatchEvaluationResult& batchResult,
    size_t rowIdxInBatch, size_t rowIdxTotal);

// Bundles the state `instantiateBatch` needs to deduplicate triples as it
// instantiates them.
struct DeduplicationParams {
  std::reference_wrapper<ConstructDeduplicator> deduplicator_;
  std::reference_wrapper<const BatchEvaluationContext> ctx_;
};

// Instantiates all template triples for all rows in a batch. For each row,
// every triple in `tmpl.preprocessedTriples_` is instantiated; triples with
// any unbound term are silently dropped. `batchOffset` is the absolute
// row ID of the first row in the batch (used to generate unique blank node
// IDs). Triples are dropped if they are considered duplicates according to
// `deduplicationParams->deduplicator_`, see `ConstructDeduplicator.h` for
// details.
std::vector<EvaluatedTriple> instantiateBatch(
    const PreprocessedConstructTemplate& tmpl,
    const BatchEvaluationResult& batchResult, size_t batchOffset,
    std::optional<DeduplicationParams> deduplicationParams = std::nullopt);

// Format a single term to its string form.
// `includeDataType=false`: integers, decimals
//   and booleans are emitted without quotes or datatype annotation.
// `includeDataType=true`: all typed literals carry an explicit
//   `"..."^^<type>` annotation.
// Terms with `type == nullptr` (IRIs, blank nodes, vocab-indexed literals)
// are returned as-is regardless of `includeDataType`.
std::string formatTerm(const EvaluatedTermData& term, bool includeDataType);

// Formats a triple (subject, predicate, object) according to the output
// format `format`.
std::string formatTriple(const EvaluatedTriple& evaluatedTriple,
                         const ad_utility::MediaType& format);

// Formats a triple as Turtle with `FastExportStreamFormatter`, which escapes
// directly into the result instead of building a `std::string` per term. The
// result is byte-identical to `formatTriple(evaluatedTriple, turtle)`.
std::string formatTripleAsTurtleWithFastFormatter(
    const EvaluatedTriple& evaluatedTriple);

// The default target size of the strings of `formatTriplesAsTurtleInBatches`.
inline constexpr size_t FAST_TURTLE_BATCH_BYTES = 64 * 1024;

// Formats all `triples` as Turtle with `FastExportStreamFormatter`, many
// triples per returned string: a string ends before the first triple that
// might not fit into `targetBatchBytes` (a single larger triple gets a string
// of its own). The concatenation of the strings is byte-identical to the
// concatenation of `formatTriple(triple, turtle)` for all triples, but there
// is one heap allocation per batch instead of several per triple. Used for the
// Turtle export if the runtime parameter `use-fast-export-stream-formatter` is
// set.
ad_utility::InputRangeTypeErased<std::string> formatTriplesAsTurtleInBatches(
    ad_utility::InputRangeTypeErased<EvaluatedTriple> triples,
    size_t targetBatchBytes = FAST_TURTLE_BATCH_BYTES);

// Creates a `StringTriple` object. Needed for backwards compatibility with
// `ExportQueryExecutionTrees::constructQueryResultBindingsToQLeverJSON`
StringTriple createStringTriple(const EvaluatedTriple& evaluatedTriple,
                                bool includeDataType = false);

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
