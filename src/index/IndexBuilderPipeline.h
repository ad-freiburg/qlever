// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_INDEXBUILDERPIPELINE_H
#define QLEVER_SRC_INDEX_INDEXBUILDERPIPELINE_H

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/IndexBuilderTypes.h"
#include "parser/AsyncMultifileParser.h"
#include "parser/RdfParser.h"
#include "util/CachingMemoryResource.h"
#include "util/ProgressBar.h"
#include "util/Synchronized.h"

class IndexImpl;

namespace qlever::indexBuilder {

// First-pass pipeline of the index builder. Consumes batches of `TurtleTriple`
// from an `AsyncMultifileParser`, assigns local vocabulary IDs to each
// component of every triple, groups the resulting `IdTriple`s into
// partial-vocab batches of
// at most `linesPerPartial` input triples, and for each such batch writes
//
//   1. a partial vocabulary file (`.partial-vocab.words.tmp.<idx>`),
//   2. the id-mapped triples to a globally shared on-disk
//      `Synchronized<unique_ptr<TripleVec>>` (in `idTriples`).
//
// The implementation is built on `boost::asio` with a dedicated
// `net::thread_pool` sized to `numThreads` (deduced from
// `std::thread::hardware_concurrency()` when not specified). Work is
// load-balanced across `M` workers; each worker owns its own `ItemMapManager`
// (no locks). Partial-vocab batch boundaries are propagated through per-worker
// channels as sentinels. A reorder buffer in the gatherer collects the `M`
// per-batch maps and dispatches finalize jobs in monotonically increasing
// `batchIdx` order. The number of finalize jobs in flight is bounded by
// `NUM_INFLIGHT_PARTIAL_VOCAB_WRITES`.
//
// The class is single-use: construct, call `run()` once, then destroy.
class IndexBuilderPipeline {
 public:
  using TripleVec =
      ad_utility::CompressedExternalIdTable<NumColumnsIndexBuilding>;
  using IdTripleArray = std::array<Id, NumColumnsIndexBuilding>;

  // Result of one finalize batch (in `batchIdx` order after `run()`).
  struct PartialResult {
    size_t numTriplesInPartial_ = 0;
  };

  // `numThreads` is the size of the dedicated thread pool used by the
  // pipeline. If `std::nullopt`, it is deduced from
  // `std::thread::hardware_concurrency()`.
  IndexBuilderPipeline(
      IndexImpl* index,
      std::shared_ptr<qlever::parser::AsyncMultifileParser> parser,
      size_t linesPerPartial, std::string onDiskBase,
      ad_utility::Synchronized<std::unique_ptr<TripleVec>>* idTriples,
      std::atomic<size_t>* numHasWordTriples,
      ad_utility::ProgressBar* progressBar, size_t* numTriplesParsedRef,
      std::optional<size_t> numThreads = std::nullopt);

  ~IndexBuilderPipeline();

  // Non-copyable, non-movable: owns a thread_pool + channels.
  IndexBuilderPipeline(const IndexBuilderPipeline&) = delete;
  IndexBuilderPipeline& operator=(const IndexBuilderPipeline&) = delete;

  // Drive the pipeline to completion. Throws if any stage failed (the first
  // captured exception is rethrown). Returns the per-partial-vocab triple
  // counts in `batchIdx` order, suitable for use as
  // `BuildPartialVocabulariesResult::numTriplesPerPartialVocab_`.
  std::vector<size_t> run();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace qlever::indexBuilder

#endif  // QLEVER_SRC_INDEX_INDEXBUILDERPIPELINE_H
