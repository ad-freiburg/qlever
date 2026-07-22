// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "ConstructDeduplicationFilter.h"

namespace qlever::constructExport {

//______________________________________________________________________________
bool PerTripleFilter::insert(const DeduplicationKey& key) {
  return std::visit(
      OverloadCallOperator{
          [&key](LruDeduplicationCache& lru) { return lru.insert(key); },
          [&key](HashSetWithMemoryLimit<DeduplicationKey>& set) {
            return set.insert(key).second;
          }},
      filter_);
}

//______________________________________________________________________________
PerTripleFilter::Filter PerTripleFilter::makeFilter(
    const DeduplicationMode& mode,
    const QueryExecutionContext& queryExecutionContext) {
  return std::visit(
      OverloadCallOperator{
          [](const DeduplicationMode::None&) -> Filter {
            AD_CONTRACT_CHECK(false,
                              "No filter is constructed for `None` mode.");
          },
          [&queryExecutionContext](const DeduplicationMode::Global&) -> Filter {
            return HashSetWithMemoryLimit<DeduplicationKey>{
                queryExecutionContext.getAllocator()};
          },
          [](const DeduplicationMode::BatchWise& batchWise) -> Filter {
            return LruDeduplicationCache{batchWise.batchSize_};
          }},
      mode.value_);
}

//______________________________________________________________________________
ConstructDeduplicationState::ConstructDeduplicationState(
    const DeduplicationMode& mode,
    const QueryExecutionContext& queryExecutionContext,
    std::optional<ad_utility::MemorySize> maxDedupVocabSize)
    : mode_{mode},
      queryExecutionContext_{queryExecutionContext},
      maxDedupVocabBytes_{computeMaxDedupVocabBytes(mode, queryExecutionContext,
                                                    maxDedupVocabSize)},
      filter_{mode, queryExecutionContext} {
  AD_CONTRACT_CHECK(
      !std::holds_alternative<DeduplicationMode::None>(mode.value_),
      "`ConstructDeduplicationState` must not be constructed for `None` mode; "
      "the caller handles `None` by not constructing it.");
}

//______________________________________________________________________________
DeduplicationKey ConstructDeduplicationState::makeFullTripleKey(
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
  return key;
}

//______________________________________________________________________________
bool ConstructDeduplicationState::isNew(
    size_t templateTripleIdx, size_t rowIdxInIdTable,
    const PreprocessedConstructTemplate& tmpl,
    const BatchEvaluationContext& ctx) {
  if (tmpl.tripleContainsBlankNode_[templateTripleIdx]) {
    return true;
  }
  resetIfVocabTooLarge();
  return filter_.insert(makeFullTripleKey(
      tmpl.preprocessedTriples_[templateTripleIdx], rowIdxInIdTable, ctx));
}

//______________________________________________________________________________
void ConstructDeduplicationState::seedGroundTriple(
    const DeduplicationKey& key) {
  filter_.insert(canonicalizeKey(key));
}

// Approximate size of a single full-triple dedup key: three `ValueId`s. Used to
// relate the `dedupVocab_` budget to the number of keys the filter itself
// holds.
static constexpr size_t bytesPerDedupKey =
    NUM_TRIPLE_POSITIONS * sizeof(ValueId);

// The byte threshold for `dedupVocab_`: the explicit `maxDedupVocabSize` if
// given, else a mode-dependent default.
size_t ConstructDeduplicationState::computeMaxDedupVocabBytes(
    const DeduplicationMode& mode,
    const QueryExecutionContext& queryExecutionContext,
    std::optional<ad_utility::MemorySize> maxDedupVocabSize) {
  if (maxDedupVocabSize.has_value()) {
    return maxDedupVocabSize.value().getBytes();
  }
  if (const auto* batchWise =
          std::get_if<DeduplicationMode::BatchWise>(&mode.value_)) {
    return batchWise->batchSize_ * bytesPerDedupKey;
  }
  return (queryExecutionContext.getAllocator().amountMemoryLeft() / 4)
      .getBytes();
}

//______________________________________________________________________________
ValueId ConstructDeduplicationState::canonicalize(ValueId id) {
  if (id.getDatatype() != Datatype::LocalVocabIndex) return id;  // fast path
  const auto& entry = *id.getLocalVocabIndex();
  size_t sizeBefore = dedupVocab_.size();
  auto index = dedupVocab_.getIndexAndAddIfNotContained(entry);
  if (dedupVocab_.size() != sizeBefore) {  // a new string was actually added
    dedupVocabBytes_ += entry.toStringRepresentation().size();
  }
  return ValueId::makeFromLocalVocabIndex(index);
}

//______________________________________________________________________________
DeduplicationKey ConstructDeduplicationState::canonicalizeKey(
    DeduplicationKey key) {
  for (ValueId& id : key) {
    id = canonicalize(id);
  }
  return key;
}

//______________________________________________________________________________
void ConstructDeduplicationState::resetIfVocabTooLarge() {
  // Only `BatchWise` bounds its vocab here; `Global` must stay exact and is
  // never reset.
  if (!std::holds_alternative<DeduplicationMode::BatchWise>(mode_.value_)) {
    return;
  }
  if (dedupVocabBytes_ < maxDedupVocabBytes_) return;
  filter_ = PerTripleFilter{mode_, queryExecutionContext_};
  dedupVocab_ = LocalVocab();
  dedupVocabBytes_ = 0;
}

}  // namespace qlever::constructExport
