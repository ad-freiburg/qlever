//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_LOCALVOCABENTRY_H
#define QLEVER_SRC_INDEX_LOCALVOCABENTRY_H

#include <gtest/gtest_prod.h>

#include <atomic>
#include <string_view>
#include <type_traits>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/keywords.h"
#include "backports/three_way_comparison.h"
#include "global/TypedIndex.h"
#include "global/VocabIndex.h"
#include "index/IdBitRepresentation.h"
#include "parser/LiteralOrIri.h"
#include "util/CopyableSynchronization.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

// The interface that this class needs from the index it belongs to. Only
// forward-declared here, because its definition requires `global/Id.h`, which
// in turn requires this header (see the note on `IdProxy` below).
class LocalVocabContext;

// The only class that may call the private constructors of `LocalVocabEntry`
// (see there). It is defined in `index/TripleComponentConversions.cpp` and must
// not be used anywhere else.
namespace localVocabEntryDetail::onlyForTripleComponentConversions {
class LocalVocabEntryFactory;
}

class LocalVocabEntry;
class LocalVocabEntryView;

namespace localVocabEntryDetail {
// Note: The values here actually are `Id`s, but we cannot store the `Id` type
// directly because of cyclic dependencies. This is defined outside of
// `BasicLocalVocabEntry`, so that both instantiations of it use the same type.
static constexpr ad_utility::IndexTag proxyTag = "LveIdProxy";
using IdProxy = ad_utility::TypedIndex<IdBitRepresentation, proxyTag>;

// The position of a word in the vocabulary, see
// `BasicLocalVocabEntry::positionInVocab` below. Also defined outside of the
// template for the same reason as `IdProxy`.
struct PositionInVocab {
  IdProxy lowerBound_;
  IdProxy upperBound_;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(PositionInVocab, lowerBound_,
                                              upperBound_)
};

// The common implementation of `LocalVocabEntry` (which owns its string) and
// `LocalVocabEntryView` (which doesn't, and is the type of the words that are
// stored in a `LocalVocab`, see below). It consists of a `LiteralOrIriT` (a
// `LiteralOrIri` or a `LiteralOrIriView`) and a cache to store the position,
// where the entry would be in the global vocabulary of the Index. This position
// is used for efficient comparisons between entries in the local and global
// vocabulary because we only have to look up the position once per
// `LocalVocabEntry`, and all subsequent comparisons are cheap.
//
// WARNING: The order that `positionInVocab()` and `compareThreeWay()` implement
// is the order in which the index scans emit their `Id`s (call it the
// *internal* order, see `ValueId::compareThreeWay`). Without a secondary
// vocabulary (see `index/vocabulary/SecondaryVocabulary.h`) that order
// coincides with the semantic (that is, by string value) order, except for the
// encoded IRIs (see `LocalVocabContext::encodeAsId`), which are ordered by
// their encoding (a pre-existing deviation that is tracked separately, see the
// note at `valueIdComparators::detail::compareIdsImpl`). As soon as an index
// has such a vocabulary, the two
// orders differ much more fundamentally: a word of the secondary vocabulary is
// positioned after *all* words of the main vocabulary, no matter what it is. An
// entry whose word is stored in the secondary vocabulary therefore compares
// greater than every word of the main vocabulary, and greater than every entry
// that is contained in neither vocabulary — even if its string value is
// smaller. Both `compareThreeWay()` and the `Id` comparison then deviate
// *silently* from the semantics that SPARQL requires, which breaks all kinds of
// semantic comparisons (`FILTER`, `ORDER BY`, the range filters and
// prefilters), see the detailed note at
// `valueIdComparators::detail::compareIdsImpl`.
//
// This is a known limitation for now: apart from unit tests, a secondary
// vocabulary is only created when a blob of `NamedCachedQueryBlobManager` that
// contains new words is loaded, so only queries on such a blob are affected.
// It has to be fixed before a secondary vocabulary is used more widely,
// most likely by keeping the position in the main vocabulary (which is what a
// semantic comparison needs, and which can always be computed from the word)
// separately from the position in the internal order, and by exposing the two
// orders as two explicitly named comparisons instead of a single
// `compareThreeWay`.
// TODO<joka921> Do that in a follow-up PR.
template <typename LiteralOrIriT>
class alignas(16) BasicLocalVocabEntry : public LiteralOrIriT {
 public:
  using Base = LiteralOrIriT;
  using IdProxy = localVocabEntryDetail::IdProxy;

 private:
  // The owning and the non-owning entries copy each other's caches.
  template <typename>
  friend class BasicLocalVocabEntry;

  // Pointer to keep this object assignable.
  const LocalVocabContext* context_;

 protected:
  // The cache for the position in the vocabulary. As usual, the `lowerBound` is
  // inclusive, the `upperBound` is not, so if `lowerBound == upperBound`, then
  // the entry is not part of the globalVocabulary, and `lowerBound` points to
  // the first *larger* word in the vocabulary. Note that the position may also
  // be in the secondary vocabulary of the index, in which case it is an `Id` of
  // type `Datatype::SecondaryVocabIndex` — see the warning in the class comment
  // above for why that makes this position currently unsuitable for semantic
  // comparisons.
  // Note: we store the cache as three separate atomics to avoid mutexes. The
  // downside is, that in parallel code multiple threads might look up the
  // position concurrently, which wastes a bit of resources. However, we don't
  // consider this case to be likely.
  // TODO<pas-kes> For the split layout `IdProxy` has 16 bytes, so these atomics
  // are not lock-free everywhere (`libatomic` on Linux). Check the alternatives
  // (once-pattern with plain fields, or separate atomics for datatype and
  // payload).
  mutable ad_utility::CopyableAtomic<IdProxy> lowerBoundInVocab_;
  mutable ad_utility::CopyableAtomic<IdProxy> upperBoundInVocab_;
  mutable ad_utility::CopyableAtomic<bool> positionInVocabKnown_ = false;

 protected:
  BasicLocalVocabEntry(Base base, const LocalVocabContext& context) noexcept
      : Base{std::move(base)}, context_{&context} {}

  // Constructor for when the position in the vocab is already known. Note that
  // the caller has to guarantee that `lower` and `upper` are exactly the bounds
  // that `positionInVocabExpensiveCase` would compute, which the expensive
  // check below verifies. In particular, a word that is contained in neither
  // the main nor the secondary vocabulary still has to be given the bounds of
  // the position at which it would be sorted into the main vocabulary.
  //
  // NOTE: The only caller outside of the tests is `toValueId` (see
  // `index/TripleComponentConversions.h`), which satisfies this by
  // construction: it obtains the bounds from the very lookup that
  // `positionInVocabExpensiveCase` performs (see
  // `LocalVocabContext::lookupWordInVocabularies`), and it only reaches this
  // constructor for a word that is in none of the vocabularies. The check below
  // is hence a tautology for that caller, and it is kept for the tests, which
  // may pass arbitrary bounds.
  template <typename Lower, typename Upper>
  BasicLocalVocabEntry(Base&& base, Lower lower, Upper upper,
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

  // Construct from `base`, which must be equal to `other` (e.g. a copy or a
  // view of it), and take over the context and the cached position in the
  // vocabulary of `other`.
  template <typename Other>
  BasicLocalVocabEntry(Base base, const BasicLocalVocabEntry<Other>& other)
      : Base{std::move(base)},
        context_{other.context_},
        lowerBoundInVocab_{other.lowerBoundInVocab_},
        upperBoundInVocab_{other.upperBoundInVocab_},
        positionInVocabKnown_{other.positionInVocabKnown_} {}

 public:
  BasicLocalVocabEntry(const BasicLocalVocabEntry&) = default;
  BasicLocalVocabEntry(BasicLocalVocabEntry&&) noexcept = default;
  BasicLocalVocabEntry& operator=(const BasicLocalVocabEntry&) = default;
  BasicLocalVocabEntry& operator=(BasicLocalVocabEntry&&) noexcept = default;

  // Slice to base class `LiteralOrIriT`.
  const Base& asLiteralOrIri() const { return *this; }

  // Return the position in the vocabulary. If it is not already cached, then
  // the call to `positionInVocab()` first computes the position and then
  // caches it.
  // Note: We use `lowerBound` and `upperBound` because depending on the Local
  // settings there might be a range of words that are considered equal for the
  // purposes of comparing and sorting them.
  //
  // The bounds are `Id`s of type `VocabIndex` or `SecondaryVocabIndex` (the
  // latter only if the index has a secondary vocabulary that contains this
  // word, see the warning in the class comment above), and this entry compares
  // exactly like an `Id` at that position. They are never of type `EncodedVal`,
  // because an IRI that is encoded directly in an `Id` is never stored in a
  // `LocalVocabEntry` (see `toIdOrLocalVocabEntry`).
  // NOTE: An `Id` of an unrelated datatype (like `Int` or `Date`) is
  // deliberately *not* compared to this position, but to the datatype bits of
  // the `LocalVocabIndex`, which yields the same result, see the note at
  // `ValueId::compareThreeWay`.
  using PositionInVocab = localVocabEntryDetail::PositionInVocab;
  PositionInVocab positionInVocab() const {
    // Immediately return if we have previously computed and cached the
    // position.
    if (positionInVocabKnown_.load(std::memory_order_acquire)) {
      return {lowerBoundInVocab_.load(std::memory_order_relaxed),
              upperBoundInVocab_.load(std::memory_order_relaxed)};
    }
    return positionInVocabExpensiveCase();
  }

  // It suffices to hash the base class `LiteralOrIriT` as the position in the
  // vocab is redundant for those purposes. NOTE: The hashes of a
  // `LocalVocabEntry` and a `LocalVocabEntryView` of the same word are equal.
  template <typename H, typename V>
  friend auto AbslHashValue(H h, const V& entry)
      -> CPP_ret(H)(
          requires ql::concepts::derived_from<V, BasicLocalVocabEntry>) {
    return H::combine(std::move(h), static_cast<const Base&>(entry));
  }

  // Compare two entries in the internal order (see the warning in the class
  // comment above; in particular this is NOT a semantic comparison as soon as
  // the index has a secondary vocabulary). If the index has such a secondary
  // vocabulary, then this first compares the positions in the vocabularies (see
  // `positionInVocab()`) and only falls back to the comparison of the strings
  // if those positions are equal. Comparing the strings alone would then not be
  // a valid strict weak ordering: a word that is stored in the secondary
  // vocabulary of the index is positioned after all words of the main
  // vocabulary, so comparing it to a word that is in neither vocabulary has to
  // yield the same result as comparing the corresponding `Id`s, which are
  // compared by their positions.
  //
  // If the index has no secondary vocabulary, then the comparison of the
  // strings alone already yields the same result as the comparison of the
  // positions, so the position is deliberately NOT looked up
  // (`LocalVocabContext` has the cheap `hasSecondaryVocabulary()` for exactly
  // this purpose). This matters because that lookup goes to the (on-disk)
  // vocabulary of the index, which the comparison of two entries would
  // otherwise require even for a workload that only ever compares entries of a
  // `LocalVocab` to each other, and never to an `Id` of the index (which needs
  // the position anyway).
  //
  // NOTE: `operator==` below never looks up the position, no matter which
  // vocabularies exist. Owning entries and views can be compared with each
  // other.
  template <typename Other>
  ql::strong_ordering compareThreeWay(
      const BasicLocalVocabEntry<Other>& rhs) const;

  // Two entries are equal if and only if their string representations are, so
  // compare those instead of going through `compareThreeWay`, which would use
  // the much more expensive collation of the vocabulary, and which would
  // additionally look up the position in the vocabulary if the index has a
  // secondary vocabulary (see the note above). This is what happens implicitly
  // anyway (the operators defined by the macro in the derived classes do not
  // include `operator==`), but state it explicitly so that it doesn't silently
  // change when `compareThreeWay` does. Owning entries and views can be
  // compared with each other.
  template <typename Other>
  bool operator==(const BasicLocalVocabEntry<Other>& rhs) const {
    return std::string_view{this->toStringRepresentation()} ==
           std::string_view{rhs.toStringRepresentation()};
  }
  template <typename Other>
  bool operator!=(const BasicLocalVocabEntry<Other>& rhs) const {
    return !(*this == rhs);
  }
  // Declaring the operators above hides the ones inherited from `Base`, so
  // also allow comparisons against a plain `LiteralOrIri` or
  // `LiteralOrIriView`, which are equal if their string representations are.
  template <bool isOwning>
  bool operator==(
      const ad_utility::triple_component::BasicLiteralOrIri<isOwning>& rhs)
      const {
    return std::string_view{this->toStringRepresentation()} ==
           std::string_view{rhs.toStringRepresentation()};
  }
  template <bool isOwning>
  bool operator!=(
      const ad_utility::triple_component::BasicLiteralOrIri<isOwning>& rhs)
      const {
    return !(*this == rhs);
  }

  // Expose `context_` for testing.
  const LocalVocabContext& getContextForTesting() const { return *context_; }

 protected:
  // Check that this entry can be anchored to `newContext`, see the
  // `reanchoredTo` functions of the derived classes.
  void checkCanBeReanchoredTo(const LocalVocabContext& newContext) const;

 private:
  // The expensive case of looking up the position in vocab.
  PositionInVocab positionInVocabExpensiveCase() const;
};
}  // namespace localVocabEntryDetail

// An owning entry, which is used for words that are not (yet) stored in a
// `LocalVocab`, for example the results of string expressions or constants of a
// query. See `BasicLocalVocabEntry` above for the details.
class alignas(16) LocalVocabEntry
    : public localVocabEntryDetail::BasicLocalVocabEntry<
          ad_utility::triple_component::LiteralOrIri> {
 private:
  using EntryBase = localVocabEntryDetail::BasicLocalVocabEntry<
      ad_utility::triple_component::LiteralOrIri>;

 public:
  using Base = EntryBase::Base;
  using IdProxy = EntryBase::IdProxy;
  using PositionInVocab = EntryBase::PositionInVocab;

  LocalVocabEntry(LiteralT literal, const LocalVocabContext& context)
      : EntryBase{Base{std::move(literal)}, context} {}

 private:
  // The constructors that can store an IRI are private, because an IRI that can
  // be encoded directly in an `Id` (see `LocalVocabContext::encodeAsId`) must
  // never be stored in a `LocalVocabEntry`. Create such entries via
  // `toIdOrLocalVocabEntry` (see `index/TripleComponentConversions.h`), which
  // checks this and then calls them via `LocalVocabEntryFactory`.
  friend class localVocabEntryDetail::onlyForTripleComponentConversions::
      LocalVocabEntryFactory;
  // Creates entries from existing entries (which already satisfy the above).
  template <typename>
  friend class localVocabEntryDetail::BasicLocalVocabEntry;
  friend class LocalVocabEntryView;

  LocalVocabEntry(IriT iri, const LocalVocabContext& context) noexcept
      : EntryBase{Base{std::move(iri)}, context} {}

  // Deliberately allow implicit conversion from `LiteralOrIri`.
  LocalVocabEntry(const Base& base, const LocalVocabContext& context)
      : EntryBase{base, context} {}
  LocalVocabEntry(Base&& base, const LocalVocabContext& context) noexcept
      : EntryBase{std::move(base), context} {}

  // See the corresponding constructor of `BasicLocalVocabEntry`.
  template <typename Lower, typename Upper>
  LocalVocabEntry(Base&& base, Lower lower, Upper upper,
                  const LocalVocabContext& context)
      : EntryBase{std::move(base), lower, upper, context} {}

  // See the corresponding constructor of `BasicLocalVocabEntry`.
  template <typename Other>
  LocalVocabEntry(
      Base base,
      const localVocabEntryDetail::BasicLocalVocabEntry<Other>& other)
      : EntryBase{std::move(base), other} {}

 public:
  LocalVocabEntry(const LocalVocabEntry&) = default;
  LocalVocabEntry(LocalVocabEntry&&) noexcept = default;
  LocalVocabEntry& operator=(const LocalVocabEntry&) = default;
  LocalVocabEntry& operator=(LocalVocabEntry&&) noexcept = default;

  // Convenience functions that delegate to the corresponding static functions
  // of `LiteralT`. Unlike the private constructors above, they are public,
  // because they can only create literals, never IRIs.
  static LocalVocabEntry literalWithoutQuotes(std::string_view view,
                                              const LocalVocabContext& ctx);

  static LocalVocabEntry literalWithNormalizedContent(
      std::string_view view, const LocalVocabContext& ctx);

  // Return a copy of this entry that is anchored to `newContext`, with an empty
  // cache for the position in the vocabulary. This is used when an index is
  // replaced by a rebuilt one, which encodes exactly the same IRIs directly in
  // an `Id` (see `toIdOrLocalVocabEntry`), so an entry that was not encodable
  // before stays not encodable.
  LocalVocabEntry reanchoredTo(const LocalVocabContext& newContext) const;

  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(LocalVocabEntry)
};

// A non-owning entry. This is the type of the words that are stored in a
// `LocalVocab`, the characters of which are owned by the `LocalVocab` (see
// there), and `LocalVocabIndex` is a pointer to it. It can't be modified, and
// can only be created from a `LocalVocabEntry` (so an IRI that has to be
// encoded directly in an `Id` can't be stored in it either). See
// `BasicLocalVocabEntry` above for the details.
class alignas(16) LocalVocabEntryView
    : public localVocabEntryDetail::BasicLocalVocabEntry<
          ad_utility::triple_component::LiteralOrIriView> {
 private:
  using EntryBase = localVocabEntryDetail::BasicLocalVocabEntry<
      ad_utility::triple_component::LiteralOrIriView>;

 public:
  using Base = EntryBase::Base;
  using IdProxy = EntryBase::IdProxy;
  using PositionInVocab = EntryBase::PositionInVocab;

  // The test inspects the cached position of the words of a `LocalVocab`.
  FRIEND_TEST(TripleComponent, toValueId);

 private:
  // `LocalVocab` stores the characters of its words separately, and creates
  // the views of them via the private constructor below.
  friend class LocalVocab;

  // Create a view of `entry` (a `LocalVocabEntry` or another view), the
  // characters of which are stored in `storage`, which must have exactly the
  // contents of `entry.toStringRepresentation()`.
  // The context and the cached position in the vocabulary of `entry` are
  // copied. The view is valid as long as `storage` is.
  template <typename Other>
  LocalVocabEntryView(
      const localVocabEntryDetail::BasicLocalVocabEntry<Other>& entry,
      std::string_view storage)
      : EntryBase{Base{entry.asLiteralOrIri(), storage}, entry} {}

  // Create a view of `base` that is anchored to `context`, see `reanchoredTo`.
  LocalVocabEntryView(Base base, const LocalVocabContext& context) noexcept
      : EntryBase{base, context} {}

 public:
  // Create a view of `entry` itself, which is only valid as long as `entry` is
  // neither destroyed, nor moved or modified. This is useful for temporarily
  // representing a `LocalVocabEntry` as an `Id`, see
  // `Id::makeFromLocalVocabIndex`.
  explicit LocalVocabEntryView(const LocalVocabEntry& entry)
      : LocalVocabEntryView{entry, entry.toStringRepresentation()} {}

  LocalVocabEntryView(const LocalVocabEntryView&) = default;
  LocalVocabEntryView(LocalVocabEntryView&&) noexcept = default;
  LocalVocabEntryView& operator=(const LocalVocabEntryView&) = default;
  LocalVocabEntryView& operator=(LocalVocabEntryView&&) noexcept = default;

  // Return an owning copy of this entry (including the cached position in the
  // vocabulary).
  LocalVocabEntry toOwned() const;

  // Return a view of the same characters that is anchored to `newContext`,
  // with an empty cache for the position in the vocabulary, see
  // `LocalVocabEntry::reanchoredTo`. Unlike there, the characters are not
  // copied, so the result is only valid as long as this view is.
  LocalVocabEntryView reanchoredTo(const LocalVocabContext& newContext) const;

  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(LocalVocabEntryView)
};

// The words of a `LocalVocab` are stored in memory that is never freed
// individually, so the views must not need a destructor.
static_assert(std::is_trivially_destructible_v<LocalVocabEntryView>);

// The defaulted copy and move operations above are silently deleted if a member
// can't be copied or moved, so check that they exist (and that the moves don't
// throw).
static_assert(std::is_copy_constructible_v<LocalVocabEntry> &&
              std::is_copy_assignable_v<LocalVocabEntry> &&
              std::is_nothrow_move_constructible_v<LocalVocabEntry> &&
              std::is_nothrow_move_assignable_v<LocalVocabEntry>);
static_assert(std::is_copy_constructible_v<LocalVocabEntryView> &&
              std::is_copy_assignable_v<LocalVocabEntryView> &&
              std::is_nothrow_move_constructible_v<LocalVocabEntryView> &&
              std::is_nothrow_move_assignable_v<LocalVocabEntryView>);

#endif  // QLEVER_SRC_INDEX_LOCALVOCABENTRY_H
