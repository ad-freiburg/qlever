//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_LOCALVOCABENTRY_H
#define QLEVER_SRC_INDEX_LOCALVOCABENTRY_H

#include <gtest/gtest_prod.h>

#include <atomic>

#include "backports/algorithm.h"
#include "backports/keywords.h"
#include "backports/three_way_comparison.h"
#include "global/TypedIndex.h"
#include "global/VocabIndex.h"
#include "parser/LiteralOrIri.h"
#include "util/CopyableSynchronization.h"
#include "util/Exception.h"

class IndexImpl;
using LocalVocabContext = IndexImpl;

// This is the type we use to store literals and IRIs in the `LocalVocab`.
// It consists of a `LiteralOrIri` and a cache to store the position, where
// the entry would be in the global vocabulary of the Index. This position is
// used for efficient comparisons between entries in the local and global
// vocabulary because we only have to look up the position once per
// `LocalVocabEntry`, and all subsequent comparisons are cheap.
class alignas(16) LocalVocabEntry
    : public ad_utility::triple_component::LiteralOrIri {
 public:
  using Base = ad_utility::triple_component::LiteralOrIri;

  // Note: The values here actually are `Id`s, but we cannot store the `Id` type
  // directly because of cyclic dependencies.
  static constexpr ad_utility::IndexTag proxyTag = "LveIdProxy";
  using IdProxy = ad_utility::TypedIndex<uint64_t, proxyTag>;

  FRIEND_TEST(TripleComponent, toValueId);

 private:
  // Pointer to keep this object assignable.
  const LocalVocabContext* context_;
  // The cache for the position in the vocabulary. As usual, the `lowerBound` is
  // inclusive, the `upperBound` is not, so if `lowerBound == upperBound`, then
  // the entry is not part of the globalVocabulary, and `lowerBound` points to
  // the first *larger* word in the vocabulary. Note: we store the cache as
  // several separate atomics to avoid mutexes. The downside is, that in
  // parallel code multiple threads might look up the position concurrently,
  // which wastes a bit of resources. However, we don't consider this case to be
  // likely.
  mutable ad_utility::CopyableAtomic<IdProxy> lowerBoundInVocab_;
  mutable ad_utility::CopyableAtomic<IdProxy> upperBoundInVocab_;
  mutable ad_utility::CopyableAtomic<bool> positionInVocabKnown_ = false;
  // A second, independent cache for the position in the auxiliary vocabulary of
  // the index (see `numSmallerAuxVocabWords()` below). It is separate from the
  // cache above because it is only required for the semantically correct
  // comparison in `valueIdComparators`, whereas the bounds above are also
  // required on the much hotter path of `Id::compareThreeWay`.
  mutable ad_utility::CopyableAtomic<uint64_t> numSmallerAuxVocabWords_;
  mutable ad_utility::CopyableAtomic<bool> numSmallerAuxVocabWordsKnown_ =
      false;

 public:
  LocalVocabEntry(LiteralT literal, const LocalVocabContext& context)
      : Base{std::move(literal)}, context_{&context} {}
  LocalVocabEntry(IriT iri, const LocalVocabContext& context) noexcept
      : Base{std::move(iri)}, context_{&context} {}

  // Deliberately allow implicit conversion from `LiteralOrIri`.
  LocalVocabEntry(const Base& base, const LocalVocabContext& context)
      : Base{base}, context_{&context} {}
  LocalVocabEntry(Base&& base, const LocalVocabContext& context) noexcept
      : Base{std::move(base)}, context_{&context} {}

  // Constructor for when the position in the vocab is already known. Note that
  // the caller has to guarantee that the word is contained in neither the main
  // nor the auxiliary vocabulary, or that `lower` and `upper` are the bounds
  // that `positionInVocabExpensiveCase` would compute (which is checked by the
  // expensive check below).
  template <typename Lower, typename Upper>
  LocalVocabEntry(Base&& base, Lower lower, Upper upper,
                  const LocalVocabContext& context)
      : Base{std::move(base)},
        context_{&context},
        lowerBoundInVocab_(IdProxy::make(lower.getBits())),
        upperBoundInVocab_(IdProxy::make(upper.getBits())),
        positionInVocabKnown_(true) {
    // Check that the given bounds are correct. The extra braces are needed to
    // keep the macro expansion from interpreting the expression as two separate
    // parameters.
    AD_EXPENSIVE_CHECK((positionInVocabExpensiveCase() ==
                        PositionInVocab{IdProxy::make(lower.getBits()),
                                        IdProxy::make(upper.getBits())}));
  }

  LocalVocabEntry(const LocalVocabEntry&) = default;
  LocalVocabEntry(LocalVocabEntry&&) noexcept = default;
  LocalVocabEntry& operator=(const LocalVocabEntry&) = default;
  LocalVocabEntry& operator=(LocalVocabEntry&&) noexcept = default;

  // Convenience functions that delegate to the corresponding static functions
  // of `IriT` and `LiteralT`.
  static LocalVocabEntry fromStringRepresentation(std::string s,
                                                  const LocalVocabContext& ctx);

  static LocalVocabEntry fromIriref(std::string_view view,
                                    const LocalVocabContext& ctx);

  static LocalVocabEntry literalWithoutQuotes(std::string_view view,
                                              const LocalVocabContext& ctx);

  static LocalVocabEntry literalWithNormalizedContent(
      NormalizedStringView view, const LocalVocabContext& ctx);

  // Slice to base class `LiteralOrIri`.
  const ad_utility::triple_component::LiteralOrIri& asLiteralOrIri() const {
    return *this;
  }

  // Return the position in the vocabulary. If it is not already cached, then
  // the call to `positionInVocab()` first computes the position and then
  // caches it.
  // Note: We use `lowerBound` and `upperBound` because depending on the Local
  // settings there might be a range of words that are considered equal for the
  // purposes of comparing and sorting them.
  struct PositionInVocab {
    IdProxy lowerBound_;
    IdProxy upperBound_;

    QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(PositionInVocab, lowerBound_,
                                                upperBound_);
  };
  PositionInVocab positionInVocab() const {
    // Immediately return if we have previously computed and cached the
    // position.
    if (positionInVocabKnown_.load(std::memory_order_acquire)) {
      return {lowerBoundInVocab_.load(std::memory_order_relaxed),
              upperBoundInVocab_.load(std::memory_order_relaxed)};
    }
    return positionInVocabExpensiveCase();
  }

  // Return the number of words of the auxiliary vocabulary of the index (see
  // `AuxVocabulary`) that are smaller than this word. Note that this is a dense
  // offset across all sub-vocabularies of the auxiliary vocabulary, not an
  // `AuxVocabIndex`, so that it is directly comparable across the
  // sub-vocabularies of a split vocabulary. Together with
  // `positionInVocab()` this pins down the position of this word in the merged
  // order of the main and the auxiliary vocabulary, which is what the
  // semantically correct comparison of `Id`s in `valueIdComparators` requires.
  // Return zero if the index has no auxiliary index.
  uint64_t numSmallerAuxVocabWords() const {
    if (numSmallerAuxVocabWordsKnown_.load(std::memory_order_acquire)) {
      return numSmallerAuxVocabWords_.load(std::memory_order_relaxed);
    }
    return numSmallerAuxVocabWordsExpensiveCase();
  }

  // It suffices to hash the base class `LiteralOrIri` as the position in the
  // vocab is redundant for those purposes.
  template <typename H, typename V>
  friend auto AbslHashValue(H h, const V& entry)
      -> CPP_ret(H)(requires ranges::same_as<V, LocalVocabEntry>) {
    return H::combine(std::move(h), static_cast<const Base&>(entry));
  }

  // Compare two entries. Note that this first compares the positions in the
  // vocabulary (see `positionInVocab()`) and only falls back to the (expensive)
  // comparison of the strings if those are equal. Comparing the strings alone
  // would not be a valid strict weak ordering: for example, a word that is
  // stored in the auxiliary vocabulary of the index is positioned after all
  // words of the main vocabulary (see `AuxVocabulary`), so comparing it to a
  // word that is in neither vocabulary has to yield the same result as
  // comparing the corresponding `Id`s, which are compared by their positions.
  ql::strong_ordering compareThreeWay(const LocalVocabEntry& rhs) const;
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(LocalVocabEntry)

  // Expose `context_` for testing.
  const LocalVocabContext& getContextForTesting() const { return *context_; }

 private:
  // The expensive case of looking up the position in vocab.
  PositionInVocab positionInVocabExpensiveCase() const;

  // The expensive case of looking up the position in the auxiliary vocabulary.
  uint64_t numSmallerAuxVocabWordsExpensiveCase() const;
};

#endif  // QLEVER_SRC_INDEX_LOCALVOCABENTRY_H
