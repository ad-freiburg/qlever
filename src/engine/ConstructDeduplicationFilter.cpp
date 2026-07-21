#include "ConstructDeduplicationFilter.h"

namespace qlever::constructExport {

ConstructDeduplicationState::ConstructDeduplicationState(
    const DeduplicationMode& mode,
    const QueryExecutionContext& queryExecutionContext) {
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
  return filter_->insert(makeFullTripleKey(tmpl.preprocessedTriples_[tripleIdx],
                                           absoluteRow, ctx));
}

void ConstructDeduplicationState::seedGroundTriple(
    const DeduplicationKey& key) {
  if (filter_.has_value()) {
    filter_->insert(canonicalizeKey(key));
  }
}

ValueId ConstructDeduplicationState::canonicalize(ValueId id) {
  if (id.getDatatype() != Datatype::LocalVocabIndex) return id;  // fast path
  const auto& entry = *id.getLocalVocabIndex();
  auto index = dedupVocab_.getIndexAndAddIfNotContained(entry);
  return ValueId::makeFromLocalVocabIndex(index);
}

DeduplicationKey ConstructDeduplicationState::canonicalizeKey(
    DeduplicationKey key) {
  for (ValueId& id : key) {
    id = canonicalize(id);
  }
  return key;
}

}  // namespace qlever::constructExport
