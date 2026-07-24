// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "ConstructDeduplicationFilter.h"

namespace qlever::constructExport {

//______________________________________________________________________________
bool TripleDeduplicator::insert(const DeduplicationKey& key) {
  return std::visit(
      OverloadCallOperator{
          [&key](LruDeduplicationCache& lru) { return lru.insert(key); },
          [&key](HashSetWithMemoryLimit<DeduplicationKey>& set) {
            return set.insert(key).second;
          }},
      deduplicator_);
}

//______________________________________________________________________________
TripleDeduplicator::Deduplicator TripleDeduplicator::makeDeduplicator(
    const DeduplicationMode& mode,
    const QueryExecutionContext& queryExecutionContext) {
  return std::visit(
      OverloadCallOperator{
          [](const DeduplicationMode::None&) -> Deduplicator {
            AD_CONTRACT_CHECK(false,
                              "No `Deduplicator` may be constructed for "
                              "`DeduplicationMode::None`.");
          },
          [&queryExecutionContext](
              const DeduplicationMode::Global&) -> Deduplicator {
            return HashSetWithMemoryLimit<DeduplicationKey>{
                queryExecutionContext.getAllocator()};
          },
          [](const DeduplicationMode::BatchWise& batchWise) -> Deduplicator {
            return LruDeduplicationCache{batchWise.batchSize_};
          }},
      mode.value_);
}

//______________________________________________________________________________
ConstructDeduplicator::ConstructDeduplicator(
    const DeduplicationMode& mode,
    const QueryExecutionContext& queryExecutionContext,
    std::optional<ad_utility::MemorySize> maxDedupVocabSize)
    : mode_{mode},
      queryExecutionContext_{queryExecutionContext},
      maxDedupVocabBytes_{computeMaxDedupVocabBytes(mode, maxDedupVocabSize)},
      filter_{mode, queryExecutionContext} {}

//______________________________________________________________________________
DeduplicationKey ConstructDeduplicator::makeFullTripleKey(
    const PreprocessedTriple& triple, size_t rowIdxInIdTable,
    const BatchEvaluationContext& ctx) {
  auto toDedupId = OverloadCallOperator{
      [](const PrecomputedConstant& c) {
        AD_CORRECTNESS_CHECK(!c.dedupId_.isUndefined());
        return c.dedupId_;
      },
      [&ctx, rowIdxInIdTable](const PrecomputedVariable& v) {
        return ctx.idTable_[rowIdxInIdTable][v.columnIndex_];
      },
      [](const PrecomputedBlankNode&) -> ValueId {
        // Blank-node triples bypass deduplication, so
        // their key is never built.
        AD_FAIL();
      }};

  DeduplicationKey key;
  for (auto&& [out, term] : ranges::views::zip(key, triple)) {
    out = canonicalize(std::visit(toDedupId, term));
  }
  resetIfVocabTooLarge();
  return key;
}

//______________________________________________________________________________
bool ConstructDeduplicator::isNew(size_t templateTripleIdx,
                                  size_t rowIdxInIdTable,
                                  const PreprocessedConstructTemplate& tmpl,
                                  const BatchEvaluationContext& ctx) {
  if (tmpl.tripleContainsBlankNode_[templateTripleIdx]) {
    return true;
  }
  // Reset only at a triple boundary, never mid-key (would dangle the key).
  resetIfVocabTooLarge();
  return filter_.insert(makeFullTripleKey(
      tmpl.preprocessedTriples_[templateTripleIdx], rowIdxInIdTable, ctx));
}

//______________________________________________________________________________
void ConstructDeduplicator::seedGroundTriple(const DeduplicationKey& key) {
  // Reset only at a triple boundary, never mid-key (would dangle the key).
  resetIfVocabTooLarge();
  filter_.insert(canonicalizeKey(key));
}

// Approximate size of a single full-triple dedup key: three `ValueId`s. Used to
// relate the `dedupVocab_` budget to the number of keys the filter itself
// holds.
static constexpr size_t bytesPerDedupKey =
    NUM_TRIPLE_POSITIONS * sizeof(ValueId);

// The byte threshold for `dedupVocab_`: the explicit `maxDedupVocabSize` if
// given, else default value dependent on `DeduplicationMode`.
size_t ConstructDeduplicator::computeMaxDedupVocabBytes(
    const DeduplicationMode& mode,
    std::optional<ad_utility::MemorySize> maxDedupVocabSize) {
  if (maxDedupVocabSize.has_value()) {
    return maxDedupVocabSize.value().getBytes();
  }
  if (const auto* batchWise =
          std::get_if<DeduplicationMode::BatchWise>(&mode.value_)) {
    return batchWise->batchSize_ * bytesPerDedupKey;
  }
  // We do not track the memory usage of the `dedupVocab_` for
  // `DeduplicationMode::Global` explicitly, thus this threshold is unused.
  // Return a dummy `0`.
  return 0;
}

//______________________________________________________________________________
ValueId ConstructDeduplicator::canonicalize(ValueId id) {
  if (id.getDatatype() != Datatype::LocalVocabIndex) {
    return id;
  }  // fast path
  const auto& entry = *id.getLocalVocabIndex();
  size_t sizeBefore = dedupVocab_.size();
  auto index = dedupVocab_.getIndexAndAddIfNotContained(entry);
  // Only `BatchWise` bounds the size of `dedupVocab_`, so only there do we
  // track the vocab's byte size and check the threshold. We do not track the
  // size of the `dedupVocab_` for `Global` mode, but delegate it in this case
  // to the `LocalVocab` class itself.
  bool addedNewString = dedupVocab_.size() != sizeBefore;
  if (isBatchWise() && addedNewString) {
    dedupVocabBytes_ += entry.toStringRepresentation().size();
  }
  return ValueId::makeFromLocalVocabIndex(index);
}

//______________________________________________________________________________
DeduplicationKey ConstructDeduplicator::canonicalizeKey(DeduplicationKey key) {
  for (ValueId& id : key) {
    id = canonicalize(id);
  }
  return key;
}

//______________________________________________________________________________
void ConstructDeduplicator::resetIfVocabTooLarge() {
  // Only `BatchWise` bounds its vocab here; `Global` must stay exact and is
  // never reset.
  if (!isBatchWise()) {
    return;
  }
  if (dedupVocabBytes_ < maxDedupVocabBytes_) {
    return;
  }
  filter_ = TripleDeduplicator{mode_, queryExecutionContext_};
  dedupVocab_ = LocalVocab();
  dedupVocabBytes_ = 0;
}

}  // namespace qlever::constructExport
