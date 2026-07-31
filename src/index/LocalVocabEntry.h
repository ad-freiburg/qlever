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

// The interface that this class needs from the index it belongs to. Only
// forward-declared here, because its definition requires `global/Id.h`, which
// in turn requires this header (see the note on `IdProxy` below).
class LocalVocabContext;

// This is the type we use to store literals and IRIs in the `LocalVocab`.
// It consists of a `LiteralOrIri` and a cache to store the position, where
// the entry would be in the global vocabulary of the Index. This position is
// used for efficient comparisons between entries in the local and global
// vocabulary because we only have to look up the position once per
// `LocalVocabEntry`, and all subsequent comparisons are cheap.
//
// WARNING: The order that `positionInVocab()` and `compareThreeWay()` implement
// is the order in which the index scans emit their `Id`s (call it the
// *internal* order, see `ValueId::compareThreeWay`). Without an auxiliary
// vocabulary (see `index/vocabulary/AuxVocabulary.h`) that order coincides with
// the semantic (that is, by string value) order, but as soon as an index has
// such a vocabulary, the two differ: a word of the auxiliary vocabulary is
// positioned after *all* words of the main vocabulary, no matter what it is. An
// entry whose word is stored in the auxiliary vocabulary therefore compares
// greater than every word of the main vocabulary, and greater than every entry
// that is contained in neither vocabulary — even if its string value is
// smaller. Both `compareThreeWay()` and the `Id` comparison then deviate
// *silently* from the semantics that SPARQL requires, which breaks all kinds of
// semantic comparisons (`FILTER`, `ORDER BY`, the range filters and
// prefilters), see the detailed note at
// `valueIdComparators::detail::compareIdsImpl`.
//
// This is deliberate for now: nothing but a unit test can currently create an
// auxiliary vocabulary (see `IndexImpl::setAuxVocabForTesting`), so no query is
// affected. It has to be fixed *before* the auxiliary index is wired up, most
// likely by keeping the position in the main vocabulary (which is what a
// semantic comparison needs, and which can always be computed from the word)
// separately from the position in the internal order, and by exposing the two
// orders as two explicitly named comparisons instead of a single
// `compareThreeWay`.
// TODO<joka921> Do that in a follow-up PR.
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
  // the first *larger* word in the vocabulary. Note that the position may also
  // be in the auxiliary vocabulary of the index, in which case it is an `Id` of
  // type `Datatype::AuxVocabIndex` — see the warning in the class comment above
  // for why that makes this position currently unsuitable for semantic
  // comparisons.
  // Note: we store the cache as three separate atomics to avoid mutexes. The
  // downside is, that in parallel code multiple threads might look up the
  // position concurrently, which wastes a bit of resources. However, we don't
  // consider this case to be likely.
  mutable ad_utility::CopyableAtomic<IdProxy> lowerBoundInVocab_;
  mutable ad_utility::CopyableAtomic<IdProxy> upperBoundInVocab_;
  mutable ad_utility::CopyableAtomic<bool> positionInVocabKnown_ = false;

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
  //
  // The bounds are `Id`s of type `VocabIndex`, `EncodedVal`, or `AuxVocabIndex`
  // (the latter only if the index has an auxiliary vocabulary that contains
  // this word, see the warning in the class comment above), and this entry
  // compares exactly like an `Id` at that position. In particular this also
  // holds when it is compared to an `Id` of an unrelated datatype like `Int` or
  // `Date`, which is what makes the comparison of `Id`s a valid strict weak
  // ordering, see `ValueId::compareThreeWay`.
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

  // It suffices to hash the base class `LiteralOrIri` as the position in the
  // vocab is redundant for those purposes.
  template <typename H, typename V>
  friend auto AbslHashValue(H h, const V& entry)
      -> CPP_ret(H)(requires ranges::same_as<V, LocalVocabEntry>) {
    return H::combine(std::move(h), static_cast<const Base&>(entry));
  }

  // Compare two entries in the internal order (see the warning in the class
  // comment above; in particular this is NOT a semantic comparison as soon as
  // the index has an auxiliary vocabulary). Note that this first compares the
  // positions in the vocabularies (see `positionInVocab()`) and only falls back
  // to the (expensive) comparison of the strings if those are equal. Comparing
  // the strings alone would not be a valid strict weak ordering: a word that is
  // stored in the auxiliary vocabulary of the index is positioned after all
  // words of the main vocabulary, so comparing it to a word that is in neither
  // vocabulary has to yield the same result as comparing the corresponding
  // `Id`s, which are compared by their positions.
  ql::strong_ordering compareThreeWay(const LocalVocabEntry& rhs) const;
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(LocalVocabEntry)

  // Two entries are equal if and only if their string representations are, so
  // forward to the base class instead of going through `compareThreeWay`, which
  // would use the much more expensive collation of the vocabulary. This is what
  // happens implicitly anyway (the operators defined by the macro above do not
  // include `operator==`), but state it explicitly so that it doesn't silently
  // change when `compareThreeWay` does.
  bool operator==(const LocalVocabEntry& rhs) const {
    return static_cast<const Base&>(*this) == static_cast<const Base&>(rhs);
  }
  // Declaring `operator==` above hides the ones inherited from `Base`, which
  // would otherwise make comparisons against a plain `LiteralOrIri` ill-formed.
  using Base::operator==;

  // Expose `context_` for testing.
  const LocalVocabContext& getContextForTesting() const { return *context_; }

 private:
  // The expensive case of looking up the position in vocab.
  PositionInVocab positionInVocabExpensiveCase() const;
};

#endif  // QLEVER_SRC_INDEX_LOCALVOCABENTRY_H
