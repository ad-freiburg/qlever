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

// Per-stream cache used by `formatTripleRle` to implement RLE prefix
// constant folding (see `formatTripleRle` below) over the CONSTRUCT triple
// export loop: `ConstructBatchEvaluator`'s `IdCache` already returns the
// same `EvaluatedTerm` (shared_ptr) instance for equal `Id`s within a
// batch, so a run of consecutive rows with an identical subject or
// predicate carries pointer-identical `EvaluatedTermData`. This cache
// remembers the last formatted subject/predicate string per pointer and
// lets `formatTripleRle` skip re-formatting them. The handles are owning:
// the cache outlives individual batches (it folds runs across batch
// boundaries), while a batch's triples only own their `EvaluatedTerm`s
// until the batch is consumed, so raw pointers would dangle. Consume this
// cache single-threaded in a single pass; do not share it across threads.
struct RleConstructTripleCache {
  EvaluatedTerm lastSubject_ = nullptr;
  EvaluatedTerm lastPredicate_ = nullptr;
  std::string cachedSubject_;
  std::string cachedPredicate_;
};

// Behaviorally identical to `formatTriple` (same byte output for the same
// input), but reuses `cache`'s memoized subject/predicate formatting when
// `evaluatedTriple`'s subject/predicate are the same `EvaluatedTerm`
// instance as the previous call. Guarded behind the
// `use-rle-prefix-construct-export` runtime parameter by the caller.
std::string formatTripleRle(const EvaluatedTriple& evaluatedTriple,
                            const ad_utility::MediaType& format,
                            RleConstructTripleCache& cache);

// Creates a `StringTriple` object. Needed for backwards compatibility with
// `ExportQueryExecutionTrees::constructQueryResultBindingsToQLeverJSON`
StringTriple createStringTriple(const EvaluatedTriple& evaluatedTriple,
                                bool includeDataType = false);

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
