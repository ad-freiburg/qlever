// Copyright 2020 - 2025 The QLever Authors, in particular:
//
// 2020 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2024 - 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// This file contains common classes and type definitions that are used during
// index creation.

#ifndef QLEVER_SRC_INDEX_INDEXBUILDERTYPES_H
#define QLEVER_SRC_INDEX_INDEXBUILDERTYPES_H

#include <absl/strings/str_cat.h>

#include <atomic>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/memory_resource.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/StringSortComparator.h"
#include "parser/TripleComponent.h"
#include "util/Conversions.h"
#include "util/HashMap.h"
#include "util/Serializer/Serializer.h"
#include "util/TupleHelpers.h"
#include "util/TypeTraits.h"

// An IRI or literal together with its index in the global vocabulary. This is
// used during vocabulary merging.
//
// TODO: Calling this struct `TripleComponentWithIndex` is a misnomer, as it
// holds the IRI or literal as a `std::string` and not as a `TripleComponent`.
struct TripleComponentWithIndex {
  std::string iriOrLiteral_;
  bool isExternal_ = false;
  uint64_t index_ = 0;

  [[nodiscard]] const auto& isExternal() const { return isExternal_; }
  [[nodiscard]] auto& isExternal() { return isExternal_; }
  [[nodiscard]] const auto& iriOrLiteral() const { return iriOrLiteral_; }
  [[nodiscard]] auto& iriOrLiteral() { return iriOrLiteral_; }
  bool isBlankNode() const { return ql::starts_with(iriOrLiteral_, "_:"); }

  AD_SERIALIZE_FRIEND_FUNCTION(TripleComponentWithIndex) {
    serializer | arg.iriOrLiteral_;
    serializer | arg.isExternal_;
    serializer | arg.index_;
  }
};

// An IRI or literal together with the information, whether it should be part
// of the external vocabulary.
struct PossiblyExternalizedIriOrLiteral {
  PossiblyExternalizedIriOrLiteral(TripleComponent iriOrLiteral,
                                   bool isExternal = false)
      : iriOrLiteral_{std::move(iriOrLiteral)}, isExternal_{isExternal} {}
  PossiblyExternalizedIriOrLiteral() = default;
  TripleComponent iriOrLiteral_;
  bool isExternal_ = false;

  AD_SERIALIZE_FRIEND_FUNCTION(PossiblyExternalizedIriOrLiteral) {
    serializer | arg.iriOrLiteral_;
    serializer | arg.isExternal_;
  }
};
using TripleComponentOrId = std::variant<PossiblyExternalizedIriOrLiteral, Id>;
using Triple = std::array<TripleComponentOrId, NumColumnsIndexBuilding>;

// The index of a word in the partial vocabulary in the first phase of index
// building together with its `SplitVal` (used for efficient comparisons when
// sorting).
//
// TODO: `LocalVocabIndex` is a misnomer, better call it `PartialVocabIndex` or
// something like that.
struct LocalVocabIndexAndSplitVal {
  uint64_t id_;
  TripleComponentComparator::SplitValNonOwningWithSortKey splitVal_;
};

// During the first phase of the index building, we use hash maps from entries
// in the partial vocabulary to their `LocalVocabIndexAndSplitVal` (see above).
// The hash map only stores pointers (`string_view` as the key, and the
// `LocalVocabIndexAndSplitVal` is a non-owning pointer type), so that we can
// deallocate all strings from a single batch of triples at once as soon as we
// have finished processing them.

// Allocator type for the hash map.
using ItemAlloc = ql::pmr::polymorphic_allocator<
    std::pair<const std::string_view, LocalVocabIndexAndSplitVal>>;

// The type of the hash map.
using ItemMap = ad_utility::HashMap<
    std::string_view, LocalVocabIndexAndSplitVal,
    absl::container_internal::hash_default_hash<std::string_view>,
    absl::container_internal::hash_default_eq<std::string_view>, ItemAlloc>;

// A vector that stores the same values as the hash map.
using ItemVec =
    std::vector<std::pair<std::string_view, LocalVocabIndexAndSplitVal>>;

// A buffer that very efficiently handles a set of strings that is deallocated
// at once when the buffer goes out of scope.
class MonotonicBuffer {
  std::unique_ptr<ql::pmr::monotonic_buffer_resource> buffer_ =
      std::make_unique<ql::pmr::monotonic_buffer_resource>();
  std::unique_ptr<ql::pmr::polymorphic_allocator<char>> charAllocator_ =
      std::make_unique<ql::pmr::polymorphic_allocator<char>>(buffer_.get());

 public:
  // Access to the underlying allocator.
  ql::pmr::polymorphic_allocator<char>& charAllocator() {
    return *charAllocator_;
  }
  // Append a string to the buffer and return a `string_view` that points into
  // the buffer.
  std::string_view addString(std::string_view input) {
    auto ptr = charAllocator_->allocate(input.size());
    ql::ranges::copy(input, ptr);
    return {ptr, input.size()};
  }
};

// The hash map (which only stores pointers) together with the `MonotonicBuffer`
// that manages the actual strings.
struct ItemMapAndBuffer {
  ItemMap map_;
  MonotonicBuffer buffer_;

  explicit ItemMapAndBuffer(ItemAlloc alloc) : map_{alloc} {}
  // Note: For older boost versions + compilers, we unfortunately cannot default
  // copy constructor because
  // 1. In older boost versions, the move operations of the polymorphic
  // allocators were not yet marked `noexcept`
  // 2. We definitely want this move constructor to be `noexcept`.
  // 3. GCC 8 complains if we explicitly use `noexcept = default` if the default
  // implementation wouldn't be noexcept.
  ItemMapAndBuffer(ItemMapAndBuffer&& rhs) noexcept
      : map_{std::move(rhs.map_)}, buffer_{std::move(rhs.buffer_)} {}
  // We have to delete the move-assignment as it would have the wrong semantics
  // (the monotonic buffer wouldn't be moved, this is one of the oddities of the
  // `ql::pmr` types.
  ItemMapAndBuffer& operator=(ItemMapAndBuffer&&) noexcept = delete;
};

using ItemMapArray = std::array<ItemMapAndBuffer, NUM_PARALLEL_ITEM_MAPS>;

// A hash map that assigns a unique ID for each of a set of strings. The IDs
// are assigned in an adjacent range starting from a configurable minimum ID.
// That way multiple maps can be used with non overlapping ranges.
//
// The `alignas` ensures that different instances of `ItemMapManager` used in
// different threads do not share a cache line (avoid "false sharing").
struct alignas(256) ItemMapManager {
  // Member variables.
  ItemMapAndBuffer map_;
  ad_utility::HashMap<Id, Id> specialIdMapping_;
  uint64_t minId_ = 0;
  const TripleComponentComparator* comparator_ = nullptr;

  // Construct with given minimum ID.
  explicit ItemMapManager(uint64_t minId, const TripleComponentComparator* cmp,
                          ItemAlloc alloc)
      : map_(alloc), minId_(minId), comparator_(cmp) {
    // Precompute the mapping from the `specialIds` to their norma IDs in the
    // vocabulary. This makes resolving such IRIs much cheaper.
    for (const auto& [specialIri, specialId] : qlever::specialIds()) {
      auto iriref = TripleComponent::Iri::fromIriref(specialIri);
      auto key = PossiblyExternalizedIriOrLiteral{std::move(iriref), false};
      specialIdMapping_[specialId] = getId(std::move(key));
    }
  }

  // Move the hash map out, as soon as we are done adding triples and only need
  // the actual vocabulary.
  ItemMapAndBuffer&& moveMap() && { return std::move(map_); }

  // For a given `TripleComponentOrId`, if we have seen it before, return its
  // assigned ID. Else assign it the next free ID, store it, and return it.
  Id getId(const TripleComponentOrId& keyOrId) {
    if (std::holds_alternative<Id>(keyOrId)) {
      auto id = std::get<Id>(keyOrId);
      if (id.getDatatype() != Datatype::Undefined) {
        return id;
      } else {
        // The only IDs with `Undefined` types ca be the `specialIds`.
        return specialIdMapping_.at(id);
      }
    }
    const auto& key = std::get<PossiblyExternalizedIriOrLiteral>(keyOrId);
    auto& map = map_.map_;
    auto& buffer = map_.buffer_;
    auto repr = key.iriOrLiteral_.toRdfLiteral();
    auto it = map.find(repr);
    if (it == map.end()) {
      uint64_t res = map.size() + minId_;
      // We have to first add the string to the buffer, otherwise we don't have
      // a persistent `string_view` to add to the `map`.
      auto keyView = buffer.addString(repr);
      // TODO<joka921> The LocalVocabIndexAndSplitVal should work on
      // `Literal|Iri|BlankNode` directly.
      map.try_emplace(
          keyView, LocalVocabIndexAndSplitVal{
                       res, comparator_->extractAndTransformComparableNonOwning(
                                repr, TripleComponentComparator::Level::TOTAL,
                                key.isExternal_, &buffer.charAllocator())});
      return Id::makeFromVocabIndex(VocabIndex::make(res));
    } else {
      return Id::makeFromVocabIndex(VocabIndex::make(it->second.id_));
    }
  }

  // Like `getId` but for all components of a triple at once.
  std::array<Id, NumColumnsIndexBuilding> getId(const Triple& t) {
    return std::apply(
        [this](const auto&... els) { return std::array{getId(els)...}; }, t);
  }
};

// A triple together with the language tag of its object (if any). If the object
// is a text literal, and the option to add `ql:has-word` triples is enabled,
// also store each word in the literal together with its term frequency.
struct ProcessedTriple {
  Triple triple_;
  std::string langtag_;
  ad_utility::HashMap<std::string, size_t> wordFrequencies_;
};

/**
 * @brief Get the tuple of lambda functions that is needed for the String-> Id
 * step of the Index building Pipeline
 *
 * return a tuple of <NumThreads> lambda functions, each lambda does the
 * following
 *
 * given an index idx, returns a lambda that
 * - Takes a triple and a language tag
 * - Returns IdTriples where the first entry are the Ids for the triple,
 *   the second and third entry are the Ids of the extra triples for the
 *   language filter implementation (or std::nullopt if there was no language
 * tag)
 * - in the <i-th> lambda all Ids are assigned according to itemMaps[i]
 * - if the argument maxNumberOfTriples is set correctly, the Id ranges assigned
 * by the different lambdas  never intersect
 *
 * The ItemMapMangers at *itemArrayPtr are also cleared and reset by this
 * function.
 *
 * @param itemMaps These Maps are used for assigning the ids. Their lifetime
 * must exceed that of this function's return value, since they are captured by
 * reference
 * @param maxNumberOfTriples The maximum total number of triples that will be
 * processed by all the lambdas together. Needed to correctly setup the Id
 * ranges for the individual HashMaps
 * @return A Tuple of lambda functions (see above)
 */
template <size_t NumThreads, typename IndexPtr>
auto getIdMapLambdas(
    std::array<std::optional<ItemMapManager>, NumThreads>* itemMapsPtr,
    size_t maxNumberOfTriples, const TripleComponentComparator* comp,
    IndexPtr* index, ItemAlloc alloc,
    std::atomic<size_t>* numHasWordTriples = nullptr) {
  // Create one `ItemMapManager` per thread, each with its own ID range.
  auto& itemMaps = *itemMapsPtr;
  for (size_t j = 0; j < NumThreads; ++j) {
    itemMaps[j].emplace(j * 100 * maxNumberOfTriples, comp, alloc);

    // This `reserve` is for a guaranteed upper bound that stays the same during
    // the whole index building. That's why we use the `CachingMemoryResource`
    // as an underlying memory pool for the allocator of the hash map to make
    // the allocation and deallocation of these hash maps (that are newly
    // created for each batch) much cheaper (see `CachingMemoryResource.h` and
    // `IndexImpl.cpp`).
    itemMaps[j]->map_.map_.reserve(5 * maxNumberOfTriples / NumThreads);

    // In each map, assign the first IDs to the special IRIs `ql:langtag` and
    // `ql:has-word`.
    //
    // NOTE: This is not necessary for functionality, but certain unit tests
    // currently fail without it.
    itemMaps[j]->getId(TripleComponent{
        ad_utility::triple_component::Iri::fromIriref(LANGUAGE_PREDICATE)});
    if (index->addHasWordTriples()) {
      itemMaps[j]->getId(TripleComponent{
          ad_utility::triple_component::Iri::fromIriref(HAS_WORD_PREDICATE)});
    }
  }
  using IdTriple = std::array<Id, NumColumnsIndexBuilding>;
  using IdTriples = std::vector<std::optional<IdTriple>>;

  // For a given `ItemMapManager` (specified via its index in `itemMaps`),
  // return a lambda that takes a single parsed `triple` and returns
  // `IdTriples`, which contains a processed version of the triple plus
  // additional internal triples if applicable.
  const auto itemMapLamdaCreator = [&itemMaps, index,
                                    numHasWordTriples](const size_t itemIndex) {
    return [&map = *itemMaps[itemIndex], index, numHasWordTriples](
               QL_CONCEPT_OR_NOTHING(ad_utility::Rvalue) auto&& triple) {
      // Process the given triple.
      ProcessedTriple lt = index->processTriple(AD_FWD(triple));

      // We return processed versions of: (1) the original triple, (2) two
      // internal triples for the language tag (if any), and (3) one triple for
      // each distinct word in the literal (if applicable).
      IdTriples result(3 + lt.wordFrequencies_.size());

      // First, process the original triple.
      result[0] = map.getId(lt.triple_);
      static_assert(NumColumnsIndexBuilding == 4,
                    " The following lines probably have to be changed when "
                    "the number of payload columns changes");
      auto& spoIds = *result[0];  // ids of original triple
      auto tripleGraphId = spoIds[ADDITIONAL_COLUMN_GRAPH_ID];

      // Second, if there is a language tag, add the corresponding two internal
      // triples. Give them the same graph ID as the original triple; that way,
      // our language filter optimizations also work with named graphs.
      //
      // NOTE: There is similar code in `DeltaTriples::makeInternalTriples`
      // for adding these internal triples for update triples. If you change
      // this code, you probably also have to change that one.
      if (!lt.langtag_.empty()) {
        // Get the `Id` for the language tag, e.g., `@en`.
        auto langTagId = map.getId(TripleComponent{
            ad_utility::convertLangtagToEntityUri(lt.langtag_)});
        // Get the `Id` for the special predicate, e.g., `@en@rdfs:label`.
        const auto& iri =
            std::get<PossiblyExternalizedIriOrLiteral>(lt.triple_[1])
                .iriOrLiteral_.getIri();
        auto langTaggedPredId = map.getId(TripleComponent{
            ad_utility::convertToLanguageTaggedPredicate(iri, lt.langtag_)});
        // Add the internal triple `<subject> @language@<predicate> <object>`.
        result[1].emplace(
            IdTriple{spoIds[0], langTaggedPredId, spoIds[2], tripleGraphId});
        // Add the internal triple `<object> ql:langtag <@language>`.
        result[2].emplace(IdTriple{
            spoIds[2],
            map.getId(
                TripleComponent{ad_utility::triple_component::Iri::fromIriref(
                    LANGUAGE_PREDICATE)}),
            langTagId, tripleGraphId});
      }

      // Third, if applicable, add a `ql:has-word` triple for each distinct word
      // in the literal. We abuse the graph ID field to store the term
      // frequency of the word in the literal.
      if (!lt.wordFrequencies_.empty()) {
        auto hasWordPredId = map.getId(TripleComponent{
            ad_utility::triple_component::Iri::fromIriref(HAS_WORD_PREDICATE)});
        size_t resultIndex = 3;
        for (const auto& [word, termFrequency] : lt.wordFrequencies_) {
          // Add the internal triple `<literal> ql:has-word "word"`.
          auto wordId = map.getId(TripleComponent{
              ad_utility::triple_component::Literal::fromEscapedRdfLiteral(
                  absl::StrCat("\"", word, "\""))});
          auto termFrequencyId =
              Id::makeFromInt(static_cast<int64_t>(termFrequency));
          result[resultIndex++].emplace(
              IdTriple{spoIds[2], hasWordPredId, wordId, termFrequencyId});
        }
        // Update the counter for the number of ql:has-word triples.
        if (numHasWordTriples != nullptr) {
          numHasWordTriples->fetch_add(lt.wordFrequencies_.size(),
                                       std::memory_order_relaxed);
        }
      }

      return result;
    };
  };

  // Return one of the above lambdas for each thread.
  auto itemMapLambdaTuple =
      ad_tuple_helpers::setupTupleFromCallable<NumThreads>(itemMapLamdaCreator);
  return itemMapLambdaTuple;
}

#endif  // QLEVER_SRC_INDEX_INDEXBUILDERTYPES_H
