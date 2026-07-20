#include "ConstructDeduplicationFilter.h"

namespace qlever::constructExport {

ConstructDeduplicationState::ConstructDeduplicationState(
    const DeduplicationMode& mode,
    const QueryExecutionContext& queryExecutionContext,
    std::optional<ad_utility::MemorySize> maxDedupVocabSize)
    : mode_{mode},
      queryExecutionContext_{queryExecutionContext},
      maxDedupVocabBytes_{
          computeMaxDedupVocabBytes(queryExecutionContext, maxDedupVocabSize)} {
  // Only the deduplicating modes get a filter. `None` leaves `filter_` empty
  // (the dedup path is never entered); constructing a `PerTripleFilter` for
  // it is a precondition violation (see `PerTripleFilter::makeFilter`).
  if (!std::holds_alternative<DeduplicationMode::None>(mode.value_)) {
    filter_.emplace(mode, queryExecutionContext);
  }
}

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

bool PerTripleFilter::insert(const DeduplicationKey& key) {
  return std::visit(
      OverloadCallOperator{
          [&key](LruDeduplicationCache& lru) { return lru.insert(key); },
          [&key](HashSetWithMemoryLimit<DeduplicationKey>& set) {
            return set.insert(key).second;
          }},
      filter_);
}

DeduplicationKey ConstructDeduplicationState::makeFullTripleKey(
    const PreprocessedTriple& triple, size_t absoluteRow,
    const BatchEvaluationContext& ctx) {
  DeduplicationKey key;
  for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
    key[pos] = canonicalize(std::visit(
        OverloadCallOperator{[](const PrecomputedConstant& c) {
                               AD_CORRECTNESS_CHECK(!c.dedupId_.isUndefined());
                               return c.dedupId_;
                             },
                             [&ctx, absoluteRow](const PrecomputedVariable& v) {
                               return ctx.idTable_[absoluteRow][v.columnIndex_];
                             },
                             [](const PrecomputedBlankNode&) -> ValueId {
                               // Blank-node triples bypass deduplication, so
                               // their key is never built.
                               AD_FAIL();
                             }},
        triple[pos]));
  }
  return key;
}

bool ConstructDeduplicationState::isNew(
    size_t tripleIdx, size_t absoluteRow,
    const PreprocessedConstructTemplate& tmpl,
    const BatchEvaluationContext& ctx) {
  AD_CORRECTNESS_CHECK(filter_.has_value());
  if (tmpl.tripleContainsBlankNode_[tripleIdx]) {
    return true;
  }
  resetIfVocabTooLarge();
  return filter_->insert(makeFullTripleKey(tmpl.preprocessedTriples_[tripleIdx],
                                           absoluteRow, ctx));
}

void ConstructDeduplicationState::seedGroundTriple(
    const DeduplicationKey& key) {
  if (filter_.has_value()) {
    filter_->insert(canonicalizeKey(key));
  }
}

// The byte threshold for `dedupVocab_`: the explicit `maxDedupVocabSize` if
// given, else a quarter of the query's currently available memory.
size_t ConstructDeduplicationState::computeMaxDedupVocabBytes(
    const QueryExecutionContext& queryExecutionContext,
    std::optional<ad_utility::MemorySize> maxDedupVocabSize) {
  return maxDedupVocabSize
      .value_or(queryExecutionContext.getAllocator().amountMemoryLeft() / 4)
      .getBytes();
}

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

void ConstructDeduplicationState::resetIfVocabTooLarge() {
  if (dedupVocabBytes_ < maxDedupVocabBytes_) return;
  if (std::holds_alternative<DeduplicationMode::Global>(mode_.value_)) {
    AD_THROW(
        "CONSTRUCT export with `construct-deduplication = global` exceeded "
        "its "
        "memory budget: Reduce the result size, raise the memory limit, or "
        "switch `construct-deduplication` to a `batchwise:<N>` mode (bounded "
        "memory, approximate) or `none` (no deduplication).");
  }
  // Only `BatchWise` reaches here, so rebuilding a filter is always valid.
  filter_.emplace(mode_, queryExecutionContext_);
  dedupVocab_ = LocalVocab();
  dedupVocabBytes_ = 0;
}

DeduplicationKey ConstructDeduplicationState::canonicalizeKey(
    DeduplicationKey key) {
  for (ValueId& id : key) {
    id = canonicalize(id);
  }
  return key;
}

}  // namespace qlever::constructExport
