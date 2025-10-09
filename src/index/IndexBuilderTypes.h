//  Copyright 2020, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

// Common classes / Typedefs that are used during Index Creation

#ifndef QLEVER_SRC_INDEX_INDEXBUILDERTYPES_H
#define QLEVER_SRC_INDEX_INDEXBUILDERTYPES_H

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

// An IRI or a literal together with the information, whether it should be part
// of the external vocabulary
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

using TripleComponentOrId = std::variant<PossiblyExternalizedIriOrLiteral, Id>;
// A triple + GraphId that also knows for each entry, whether this entry should
// be part of the external vocabulary.
using Triple = std::array<TripleComponentOrId, NumColumnsIndexBuilding>;

/// The index of a word and the corresponding `SplitVal`.
struct LocalVocabIndexAndSplitVal {
  uint64_t id_;
  TripleComponentComparator::SplitValNonOwningWithSortKey splitVal_;
};

// During the first phase of the index building we use hash maps from strings
// (entries in the vocabulary) to their `LocalVocabIndexAndSplitVal` (see
// above). In the hash map we will only store pointers (`string_view` as the
// key, and the `LocalVocabIndexAndSplitVal` also is a non-owning pointer type)
// and manage the memory separately, s.t. we can deallocate all the strings of a
// single phase at once as soon as we are finished with them.

// Allocator type for the hash map
using ItemAlloc = ql::pmr::polymorphic_allocator<
    std::pair<const std::string_view, LocalVocabIndexAndSplitVal>>;

// The actual hash map type.
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
  ItemMapAndBuffer(ItemMapAndBuffer&&) noexcept = default;
  // We have to delete the move-assignment as it would have the wrong semantics
  // (the monotonic buffer wouldn't be moved, this is one of the oddities of the
  // `ql::pmr` types.
  ItemMapAndBuffer& operator=(ItemMapAndBuffer&&) noexcept = delete;
};

using ItemMapArray = std::array<ItemMapAndBuffer, NUM_PARALLEL_ITEM_MAPS>;

/**
 * Manage a HashMap of string->Id to create unique Ids for strings.
 * Ids are assigned in an adjacent range starting with a configurable
 * minimum Id. That way multiple maps can be used with non overlapping ranges.
 */
// Align each ItemMapManager on its own cache line to avoid false sharing.
struct alignas(256) ItemMapManager {
  /// Construct by assigning the minimum ID that should be returned by the map.
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

  /// Move the held HashMap out as soon as we are done inserting and only need
  /// the actual vocabulary.
  ItemMapAndBuffer&& moveMap() && { return std::move(map_); }

  /// If the key was seen before, return its preassigned ID. Else assign the
  /// next free ID to the string, store and return it.
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

  /// call getId for each of the Triple elements.
  std::array<Id, NumColumnsIndexBuilding> getId(const Triple& t) {
    return std::apply(
        [this](const auto&... els) { return std::array{getId(els)...}; }, t);
  }
  ItemMapAndBuffer map_;
  ad_utility::HashMap<Id, Id> specialIdMapping_;
  uint64_t minId_ = 0;
  const TripleComponentComparator* comparator_ = nullptr;
};

/// Combines a triple (three strings) together with the (possibly empty)
/// language tag of its object.
struct LangtagAndTriple {
  std::string langtag_;
  Triple triple_;
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
 * - Returns OptionalIds where the first entry are the Ids for the triple,
 *   the second and third entry are the Ids of the extra triples for the
 *   language filter implementation (or std::nullopt if there was no language
 * tag)
 * - in the <i-th> lambda all Ids are assigned according to itemArray[i]
 * - if the argument maxNumberOfTriples is set correctly, the Id ranges assigned
 * by the different lambdas  never intersect
 *
 * The ItemMapMangers at *itemArrayPtr are also cleared and reset by this
 * function.
 *
 * @param itemArray These Maps are used for assigning the ids. Their lifetime
 * must exceed that of this function's return value, since they are captured by
 * reference
 * @param maxNumberOfTriples The maximum total number of triples that will be
 * processed by all the lambdas together. Needed to correctly setup the Id
 * ranges for the individual HashMaps
 * @return A Tuple of lambda functions (see above)
 */
template <size_t NumThreads, typename IndexPtr>
auto getIdMapLambdas(
    std::array<std::optional<ItemMapManager>, NumThreads>* itemArrayPtr,
    size_t maxNumberOfTriples, const TripleComponentComparator* comp,
    IndexPtr* indexPtr, ItemAlloc alloc) {
  // that way the different ids won't interfere
  auto& itemArray = *itemArrayPtr;
  for (size_t j = 0; j < NumThreads; ++j) {
    itemArray[j].emplace(j * 100 * maxNumberOfTriples, comp, alloc);
    // This `reserve` is for a guaranteed upper bound that stays the same during
    // the whole index building. That's why we use the `CachingMemoryResource`
    // as an underlying memory pool for the allocator of the hash map to make
    // the allocation and deallocation of these hash maps (that are newly
    // created for each batch) much cheaper (see `CachingMemoryResource.h` and
    // `IndexImpl.cpp`).
    itemArray[j]->map_.map_.reserve(5 * maxNumberOfTriples / NumThreads);
    // The LANGUAGE_PREDICATE gets the first ID in each map. TODO<joka921>
    // This is not necessary for the actual QLever code, but certain unit tests
    // currently fail without it.
    itemArray[j]->getId(TripleComponent{
        ad_utility::triple_component::Iri::fromIriref(LANGUAGE_PREDICATE)});
  }
  using OptionalIds =
      std::array<std::optional<std::array<Id, NumColumnsIndexBuilding>>, 3>;

  /* given an index idx, returns a lambda that
   * - Takes a triple and a language tag
   * - Returns OptionalIds where the first entry are the Ids for the triple,
   *   the second and third entry are the Ids of the extra triples for the
   *   language filter implementation (or std::nullopt if there was no language
   * tag)
   * - All Ids are assigned according to itemArray[idx]
   */
  const auto itemMapLamdaCreator = [&itemArray, indexPtr](const size_t idx) {
    return [&map = *itemArray[idx],
            indexPtr](QL_CONCEPT_OR_NOTHING(ad_utility::Rvalue) auto&& tr) {
      auto lt = indexPtr->tripleToInternalRepresentation(AD_FWD(tr));
      OptionalIds res;
      // get Ids for the actual triple and store them in the result.
      res[0] = map.getId(lt.triple_);
      if (!lt.langtag_.empty()) {  // the object of the triple was a literal
                                   // with a language tag
        // get the Id for the corresponding langtag Entity
        auto langTagId = map.getId(TripleComponent{
            ad_utility::convertLangtagToEntityUri(lt.langtag_)});
        // get the Id for the tagged predicate, e.g. @en@rdfs:label
        const auto& iri =
            std::get<PossiblyExternalizedIriOrLiteral>(lt.triple_[1])
                .iriOrLiteral_.getIri();
        auto langTaggedPredId = map.getId(TripleComponent{
            ad_utility::convertToLanguageTaggedPredicate(iri, lt.langtag_)});
        auto& spoIds = *res[0];  // ids of original triple
        // TODO replace the std::array by an explicit IdTriple class,
        //  then the emplace calls don't need the explicit type.
        using Arr = std::array<Id, NumColumnsIndexBuilding>;
        static_assert(NumColumnsIndexBuilding == 4,
                      " The following lines probably have to be changed when "
                      "the number of payload columns changes");
        // extra triple <subject> @language@<predicate> <object>
        // The additional triples have the same graph ID as the original triple.
        // This makes optimizations such as language filters also work with
        // named graphs. Note that we have a different mechanism in place to
        // distinguish between normal and internal triples.
        auto tripleGraphId = res[0].value()[ADDITIONAL_COLUMN_GRAPH_ID];
        res[1].emplace(
            Arr{spoIds[0], langTaggedPredId, spoIds[2], tripleGraphId});
        // extra triple <object> ql:language-tag <@language>
        res[2].emplace(Arr{spoIds[2],
                           map.getId(TripleComponent{
                               ad_utility::triple_component::Iri::fromIriref(
                                   LANGUAGE_PREDICATE)}),
                           langTagId, tripleGraphId});
      }
      return res;
    };
  };

  // setup a tuple with one lambda function per map in the itemArray
  // (the first lambda will assign ids according to itemArray[1]...
  auto itemMapLambdaTuple =
      ad_tuple_helpers::setupTupleFromCallable<NumThreads>(itemMapLamdaCreator);
  return itemMapLambdaTuple;
}

#endif  // QLEVER_SRC_INDEX_INDEXBUILDERTYPES_H
