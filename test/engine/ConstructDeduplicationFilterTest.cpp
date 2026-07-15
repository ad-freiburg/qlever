#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructDeduplicationFilter.h"
#include "global/ValueId.h"
#include "gtest/gtest.h"

namespace {

// Helper: add `s` to `vocab`, return its `Id`.
ValueId getLocalVocabId(LocalVocab& vocab, std::string_view s,
                        const QueryExecutionContext& qec) {
  return Id::makeFromLocalVocabIndex(vocab.getIndexAndAddIfNotContained(
      LocalVocabEntry::literalWithoutQuotes(s, qec.getLocalVocabContext())));
}

}  // namespace

// Case 2: equal literals from two different `LocalVocab`s collapse to one
// canonical `DeduplicationKey`.
TEST(ConstructDeduplicationFilter, crossVocabCollapse) {
  auto qec = ad_utility::testing::getQec();
  ConstructDeduplicationState state{DeduplicationMode::global(), *qec};
  PreprocessedTriple triple{PrecomputedVariable{0}, PrecomputedVariable{0},
                            PrecomputedVariable{0}};
  LocalVocab v1;
  LocalVocab v2;
  auto t1 = makeIdTableFromVector({{getLocalVocabId(v1, "\"x\"", qec)}});
  auto t2 = makeIdTableFromVector({{getLocalVocabId(v2, "\"x\"", qec)}});
  BatchEvaluationContext c1{t1.asStaticView<0>(), 0, 1};
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, 1};

  EXPECT_EQ(state.makeFullTripleKey(triple, 0, c1),
            state.makeFullTripleKey(triple, 0,
                                    c2));  // reseated to same `dedupVocab_` id.
}

// Case 3:
