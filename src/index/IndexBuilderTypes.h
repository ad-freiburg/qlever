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

#include <absl/container/inlined_vector.h>
#include <absl/strings/str_cat.h>
#include <re2/re2.h>

#include <atomic>
#include <memory>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/memory_resource.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/TripleComponentConversions.h"
#include "index/vocabulary/StringSortComparator.h"
#include "parser/TripleComponent.h"
#include "util/Conversions.h"
#include "util/HashMap.h"
#include "util/Serializer/Serializer.h"
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
  // Return true if this word is a blank node. A word is a blank node if it
  // starts with `_:`, or, when `blankNodeIriRegexes` is given, if it is an IRI
  // that is fully matched by one of those regexes.
  //
  // The regexes are matched (via `RE2::FullMatch`) against the full text of the
  // word, *including* the surrounding angle brackets of an IRI. The match has
  // to cover the entire word, so a regex must describe the whole IRI; to allow
  // an arbitrary suffix, end it with `.*`. For example the regex
  // `<https://example\.org/statement/.*>` matches the IRI
  // `<https://example.org/statement/42>`. Only IRIs (words starting with `<`)
  // are ever treated this way; literals are never converted. The regexes are
  // required to describe IRIs (i.e. to start with `<`), which is enforced by
  // `IndexImpl::setBlankNodeIriRegexes`. See also the
  // `--iri-as-blank-node-regexes` option of the index builder.
  bool isBlankNode(
      const std::vector<std::unique_ptr<re2::RE2>>& blankNodeIriRegexes) const {
    if (ql::starts_with(iriOrLiteral_, "_:")) {
      return true;
    }
    // Only IRIs (which start with `<`) can be treated as blank nodes; this also
    // avoids running the regexes for the common case of a literal.
    if (!ql::starts_with(iriOrLiteral_, "<")) {
      return false;
    }
    return ql::ranges::any_of(blankNodeIriRegexes, [this](const auto& regex) {
      return re2::RE2::FullMatch(iriOrLiteral_, *regex);
    });
  }

  AD_SERIALIZE_FRIEND_FUNCTION(TripleComponentWithIndex) {
    serializer | arg.iriOrLiteral_;
    serializer | arg.isExternal_;
    serializer | arg.index_;
  }
};

// A `TripleComponent` together with the information, whether it should be part
// of the external vocabulary.
struct PossiblyExternalizedTripleComponent {
  PossiblyExternalizedTripleComponent(TripleComponent tripleComponent,
                                      bool isExternal = false)
      : tripleComponent_{std::move(tripleComponent)}, isExternal_{isExternal} {}
  PossiblyExternalizedTripleComponent() = default;
  TripleComponent tripleComponent_;
  bool isExternal_ = false;

  AD_SERIALIZE_FRIEND_FUNCTION(PossiblyExternalizedTripleComponent) {
    serializer | arg.tripleComponent_;
    serializer | arg.isExternal_;
  }
};
using Triple =
    std::array<PossiblyExternalizedTripleComponent, NumColumnsIndexBuilding>;

// The index of a word within a partial vocabulary and the corresponding bool
// that indicates if it belongs to the external vocabulary.
// The `isExternal` bool is encoded in the most significant bit of the id which
// can never be used anyway because this is occupied by the datatype bits of the
// final `Id`.
class PartialVocabIndexWithExternalFlag {
  uint64_t encodedId_;

 public:
  PartialVocabIndexWithExternalFlag(uint64_t id, bool isExternal)
      : encodedId_{(uint64_t(isExternal) << 63) | id} {
    // The top four bits of any partial-vocab id must be zero: in the final
    // `Id` they are occupied by the datatype tag (see `ValueId::numDataBits`).
    // This guard catches future regressions that funnel a tagged value or an
    // underflowed counter through here, which would otherwise silently
    // collide with the `isExternal` bit and corrupt the vocabulary mapping.
    AD_EXPENSIVE_CHECK(id < (uint64_t{1} << ValueId::numDataBits));
  }

  PartialVocabIndexWithExternalFlag() = default;

  // Access the original values.
  uint64_t id() const { return encodedId_ & (uint64_t(-1) >> 1); }
  bool isExternal() const { return (encodedId_ >> 63) != 0; }
};

// During the first phase of the index building, we use hash maps from entries
// in the partial vocabulary to their `PartialVocabIndexWithExternalFlag` (see
// above). The hash map only stores `string_view`s as keys, so that we can
// deallocate all strings from a single batch of triples at once as soon as we
// have finished processing them.

// Allocator type for the hash map.
using ItemAlloc = ql::pmr::polymorphic_allocator<
    std::pair<const std::string_view, PartialVocabIndexWithExternalFlag>>;

// The type of the hash map.
using ItemMap =
    ad_utility::HashMap<std::string_view, PartialVocabIndexWithExternalFlag,
                        absl::DefaultHashContainerHash<std::string_view>,
                        absl::DefaultHashContainerEq<std::string_view>,
                        ItemAlloc>;

// A vector that stores the same values as the hash map.
using ItemVec =
    std::vector<std::pair<std::string_view, PartialVocabIndexWithExternalFlag>>;

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
  uint64_t minId_;
  const TripleComponentComparator* comparator_;

  // Construct with given minimum ID.
  explicit ItemMapManager(uint64_t minId, const TripleComponentComparator* cmp,
                          ItemAlloc alloc)
      : map_(alloc), minId_(minId), comparator_(cmp) {
    // Precompute the mapping from the `specialIds` to their normal IDs in the
    // vocabulary. This makes resolving such IRIs much cheaper.
    for (const auto& [specialIri, specialId] : qlever::specialIds()) {
      auto iriref = TripleComponent::Iri::fromIriref(specialIri);
      auto key = PossiblyExternalizedTripleComponent{std::move(iriref), false};
      specialIdMapping_[specialId] = getId(key);
    }
  }

  // Move the hash map out, as soon as we are done adding triples and only need
  // the actual vocabulary.
  ItemMapAndBuffer&& moveMap() && { return std::move(map_); }

  // For a given `PossiblyExternalizedTripleComponent`, if we have seen it
  // before, return its assigned ID. Else assign it the next free ID, store it,
  // and return it.
  Id getId(const PossiblyExternalizedTripleComponent& key) {
    if (key.tripleComponent_.isId()) {
      auto id = key.tripleComponent_.getId();
      if (id.getDatatype() != Datatype::Undefined) {
        return id;
      } else {
        // The only IDs with `Undefined` types ca be the `specialIds`.
        return specialIdMapping_.at(id);
      }
    }
    auto& map = map_.map_;
    auto& buffer = map_.buffer_;
    auto repr = toRdfLiteral(key.tripleComponent_);
    auto it = map.find(repr);
    if (it == map.end()) {
      uint64_t res = map.size() + minId_;
      // We have to first add the string to the buffer, otherwise we don't have
      // a persistent `string_view` to add to the `map`.
      auto keyView = buffer.addString(repr);
      map.try_emplace(keyView,
                      PartialVocabIndexWithExternalFlag{res, key.isExternal_});
      return Id::makeFromVocabIndex(VocabIndex::make(res));
    } else {
      return Id::makeFromVocabIndex(VocabIndex::make(it->second.id()));
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

// The Ids of a triple, once its string components have been mapped via an
// `ItemMapManager`. NOTE: Deliberately not named `IdTriple`, which is a class
// with a similar purpose defined in `global/IdTriple.h`.
using MappedTriple = std::array<Id, NumColumnsIndexBuilding>;
// The `MappedTriple`s that a single input triple gives rise to: one for the
// triple itself, plus the extra internal triples (for the language filter
// implementation and for the text index) that it gives rise to.
using MappedTriples = absl::InlinedVector<MappedTriple, 3>;

// Perform the String -> Id step of the Index building pipeline for a single
// triple.
//
// Returns the `MappedTriples` for `triple`, that is the Ids for the triple
// itself plus the Ids of the extra internal triples (for the language filter
// implementation and for the text index) that it gives rise to. All Ids are
// assigned according to `map`.
//
// `map` is used for assigning the ids.
template <typename IndexPtr>
MappedTriples mapTripleToIds(
    QL_CONCEPT_OR_NOTHING(ad_utility::Rvalue) auto&& triple,
    ItemMapManager& map, IndexPtr* index,
    std::atomic<size_t>* numHasWordTriples = nullptr) {
  // Process the given triple.
  ProcessedTriple lt = index->processTriple(AD_FWD(triple));

  // Reserve the exact number of triples we will produce. For ≤3 triples
  // (original + language tag), this stays inline. For more (has-word
  // triples), this allocates on the heap once.
  MappedTriples result;
  result.reserve(1 + (lt.langtag_.empty() ? 0 : 2) +
                 lt.wordFrequencies_.size());

  // First, process the original triple.
  result.push_back(map.getId(lt.triple_));
  static_assert(NumColumnsIndexBuilding == 4,
                " The following lines probably have to be changed when "
                "the number of payload columns changes");
  // Convenience reference to the IDs of the original triple. This is safe
  // because the `reserve` above ensures that no subsequent `push_back` will
  // reallocate `result`.
  auto& spoIds = result[0];
  auto tripleGraphId = spoIds[ADDITIONAL_COLUMN_GRAPH_ID];

  // Second, if there is a language tag, add the corresponding two internal
  // triples. Give them the same graph ID as the original triple; that way,
  // our language filter optimizations also work with named graphs.
  //
  // NOTE: There is similar code in `DeltaTriples::makeInternalTriples`
  // for adding these internal triples for update triples. If you change
  // this code, you probably also have to change that one. This should
  // eventually be refactored, so that this code duplication is avoided.
  if (!lt.langtag_.empty()) {
    // Get the `Id` for the language tag, e.g., `@en`.
    auto langTagId = map.getId(
        TripleComponent{ad_utility::convertLangtagToEntityUri(lt.langtag_)});
    // Get the `Id` for the special predicate, e.g., `@en@rdfs:label`.
    const auto& iri = lt.triple_[1].tripleComponent_.getIri();
    auto langTaggedPredId = map.getId(TripleComponent{
        ad_utility::convertToLanguageTaggedPredicate(iri, lt.langtag_)});
    // Add the internal triple `<subject> @language@<predicate> <object>`.
    result.push_back(
        MappedTriple{spoIds[0], langTaggedPredId, spoIds[2], tripleGraphId});
    // Add the internal triple `<object> ql:langtag <@language>`.
    result.push_back(MappedTriple{
        spoIds[2],
        map.getId(TripleComponent{
            ad_utility::triple_component::Iri::fromIriref(LANGUAGE_PREDICATE)}),
        langTagId, tripleGraphId});
  }

  // Third, if applicable, add a `ql:has-word` triple for each distinct word
  // in the literal. We abuse the graph ID field to store the term
  // frequency of the word in the literal.
  //
  // NOTE: There is similar code in `DeltaTriples::makeInternalTriples`
  // for adding these internal triples for update triples. If you change
  // this code, you probably also have to change that one. This should
  // eventually be refactored, so that this code duplication is avoided.
  if (!lt.wordFrequencies_.empty()) {
    auto hasWordPredId = map.getId(TripleComponent{
        ad_utility::triple_component::Iri::fromIriref(HAS_WORD_PREDICATE)});
    for (const auto& [word, termFrequency] : lt.wordFrequencies_) {
      // Add the internal triple `<literal> ql:has-word "word"`.
      auto wordId = map.getId(TripleComponent{
          ad_utility::triple_component::Literal::literalWithoutQuotes(word)});
      result.push_back(
          MappedTriple{spoIds[2], hasWordPredId, wordId,
                       Id::makeFromInt(static_cast<int64_t>(termFrequency))});
    }
    // Update the counter for the number of `ql:has-word` triples. Relaxed
    // ordering is fine because this counter is only read after all threads
    // have finished (for a log message).
    if (numHasWordTriples != nullptr) {
      numHasWordTriples->fetch_add(lt.wordFrequencies_.size(),
                                   std::memory_order_relaxed);
    }
  }

  return result;
}

// Return type of `IndexImpl::buildPartialVocabularies`.
struct BuildPartialVocabulariesResult {
  using TripleVec =
      ad_utility::CompressedExternalIdTable<NumColumnsIndexBuilding>;
  // The triples and partial vocabularies that a single worker thread has
  // created. The workers work completely independently of each other, so each
  // of them has its own `idTriples_`.
  struct WorkerResult {
    // The i-th entry is the actual number of triples of the i-th partial
    // vocabulary of this worker. It might be slightly different from the
    // specified `batchSize` because of internally added triples. The triples
    // appear in `idTriples_` in exactly this order.
    std::vector<size_t> numTriplesPerPartialVocab_;
    std::unique_ptr<TripleVec> idTriples_;
  };
  // One entry per worker, in the order of the worker indices.
  std::vector<WorkerResult> workerResults_;

  // The suffix of the filenames of the `partialVocabIdx`-th partial vocabulary
  // of the worker with index `workerIdx`. The partial vocabularies are named
  // after the worker that created them, so that the workers don't need a shared
  // counter for the filenames.
  static std::string partialVocabularySuffix(size_t workerIdx,
                                             size_t partialVocabIdx) {
    return absl::StrCat(workerIdx, ".", partialVocabIdx);
  }

  // The suffixes of all partial vocabularies that were written, in the order in
  // which the corresponding triples are stored (that is, first all the partial
  // vocabularies of the first worker, then those of the second worker, etc.).
  std::vector<std::string> partialVocabularySuffixes() const {
    std::vector<std::string> suffixes;
    for (size_t workerIdx = 0; workerIdx < workerResults_.size(); ++workerIdx) {
      const auto& numTriples =
          workerResults_[workerIdx].numTriplesPerPartialVocab_;
      for (size_t partialVocabIdx = 0; partialVocabIdx < numTriples.size();
           ++partialVocabIdx) {
        suffixes.push_back(partialVocabularySuffix(workerIdx, partialVocabIdx));
      }
    }
    return suffixes;
  }
};

#endif  // QLEVER_SRC_INDEX_INDEXBUILDERTYPES_H
