// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructBatchEvaluator.h"

#include <cstdlib>
#include <fstream>
#include <mutex>

#include "global/RuntimeParameters.h"
#include "index/ExportIds.h"
#include "util/Views.h"

namespace qlever::constructExport {

namespace {
// Diagnostic instrumentation for the evaluation of the batched vocabulary
// lookup: when the environment variable `QLEVER_MISSSET_LOG` is set to a file
// path, the size of every per-batch, per-column vocabulary miss set (i.e. the
// number of indices passed to a single `lookupBatch` call) is appended to that
// file, one integer per line. This characterizes the distribution of
// `lookupBatch` request sizes for a given query. It is a no-op unless the
// variable is set, so it does not affect timing measurements.
//
// The output file is owned by this RAII type: it is opened once when the single
// static instance is constructed and closed when it is destroyed at program
// exit.
class MissSetSizeLogger {
 public:
  MissSetSizeLogger() {
    if (const char* path = std::getenv("QLEVER_MISSSET_LOG")) {
      out_.open(path, std::ios::app);
    }
  }

  void log(size_t size) {
    if (!out_.is_open()) {
      return;
    }
    std::lock_guard<std::mutex> lock{mutex_};
    // Flush each record: the server is long-running, so a buffered write would
    // not reach disk until the stream is closed at program exit.
    out_ << size << std::endl;
  }

 private:
  std::ofstream out_;
  std::mutex mutex_;
};

void logMissSetSize(size_t size) {
  static MissSetSizeLogger logger;
  logger.log(size);
}
}  // namespace

// _____________________________________________________________________________
BatchEvaluationResult ConstructBatchEvaluator::evaluateBatch(
    ql::span<const size_t> variableColumnIndices,
    const BatchEvaluationContext& evaluationContext,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = evaluationContext.numRows();

  for (size_t variableColumnIdx : variableColumnIndices) {
    auto [it, wasNew] = batchResult.variablesByColumn_.emplace(
        variableColumnIdx,
        evaluateVariableByColumn(variableColumnIdx, evaluationContext,
                                 localVocab, index, idCache));
    AD_CORRECTNESS_CHECK(wasNew);
  }

  return batchResult;
}

// _____________________________________________________________________________
std::optional<EvaluatedTerm>
ConstructBatchEvaluator::stringAndTypeToEvaluatedTerm(
    std::optional<std::pair<std::string, const char*>> optStringAndType) {
  if (!optStringAndType.has_value()) return std::nullopt;
  auto& [str, type] = optStringAndType.value();
  return std::make_shared<const EvaluatedTermData>(
      EvaluatedTermData{std::move(str), type});
}

// _____________________________________________________________________________
EvaluatedVariableValues ConstructBatchEvaluator::evaluateVariableByColumn(
    size_t idTableColumnIdx, const BatchEvaluationContext& ctx,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  decltype(auto) col = ctx.idTable_.getColumn(idTableColumnIdx)
                           .subspan(ctx.firstRow_, ctx.numRows());

  const size_t numRows = ctx.numRows();

  // Build a `(rowInBatch, Id)` index vector and sort by `Id`. This ensures
  // that `VocabIndex` IDs form a contiguous, sorted block (see
  // `idsToStringAndType`), converting vocabulary lookups from random-access
  // reads to sequential reads for I/O locality.
  auto sortedIndices = ::ranges::to_vector(::ranges::views::enumerate(col));

  ql::ranges::sort(sortedIndices, {}, ad_utility::second);

  // Phase 1: check the cache for each sorted ID. Scatter hits directly to
  // `result`; collect misses for batch resolution.
  EvaluatedVariableValues result(numRows);
  // Unique `Id`s not found in `idCache`, in sorted order (inherited from
  // `sortedIndices`). Each entry corresponds to the entry at the same index
  // in `missRows`.
  std::vector<Id> missIds;
  // For each entry in `missIds`, the batch row indices that hold that `Id`.
  std::vector<absl::InlinedVector<size_t, 3>> missRows;
  for (const auto& [rowInBatch, id] : sortedIndices) {
    auto cached = idCache.tryGet(id);
    if (cached) {
      result[rowInBatch] = cached.value();
    } else if (!missIds.empty() && missIds.back() == id) {
      missRows.back().push_back(static_cast<size_t>(rowInBatch));
    } else {
      missIds.push_back(id);
      missRows.push_back({static_cast<size_t>(rowInBatch)});
    }
  }

  logMissSetSize(missIds.size());

  // Phase 2: resolve cache misses. `missIds` is deduplicated and sorted
  // (inherited from `sortedIndices`), satisfying the `idsToStringAndType`
  // precondition for sequential VocabIndex I/O. The `use-batch-vocab-lookup`
  // parameter selects between the single batched (io_uring-backed) lookup and a
  // per-ID baseline that resolves the very same misses one at a time; this
  // isolates the batched-disk-read contribution for the evaluation.
  std::vector<std::optional<std::pair<std::string, const char*>>> missResolved;
  if (getRuntimeParameter<&RuntimeParameters::useBatchVocabLookup_>()) {
    missResolved =
        ql::exportIds::idsToStringAndType(index, missIds, localVocab);
  } else {
    missResolved.reserve(missIds.size());
    for (const Id& id : missIds) {
      missResolved.push_back(
          ql::exportIds::idToStringAndType(index, id, localVocab));
    }
  }
  for (auto&& [id, resolved, rows] :
       ::ranges::views::zip(missIds, missResolved, missRows)) {
    const auto& evaluated = idCache.getOrCompute(id, [&resolved](const Id&) {
      return ConstructBatchEvaluator::stringAndTypeToEvaluatedTerm(
          std::move(resolved));
    });
    for (size_t row : rows) {
      result[row] = evaluated;
    }
  }
  return result;
}

}  // namespace qlever::constructExport
