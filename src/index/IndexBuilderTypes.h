//  Copyright 2020, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

// Common classes / Typedefs that are used during Index Creation

#include "../global/Constants.h"
#include "../global/Id.h"
#include "../util/Conversions.h"
#include "../util/HashMap.h"
#include "../util/TupleHelpers.h"
#include "./ConstantsIndexBuilding.h"
#include "./StringSortComparator.h"

#ifndef QLEVER_INDEXBUILDERTYPES_H
#define QLEVER_INDEXBUILDERTYPES_H

// A triple entry (subject, predicate, object) together with the information,
// whether it should be part of the external vocabulary
struct TripleComponent {
  TripleComponent(std::string iriOrLiteral, bool isExternal = false)
      : _iriOrLiteral{std::move(iriOrLiteral)}, _isExternal{isExternal} {}
  TripleComponent() = default;
  std::string _iriOrLiteral;
  bool _isExternal = false;

  template <typename Serializer>
  friend void serialize(Serializer& serializer, TripleComponent& entry) {
    serializer | entry._iriOrLiteral;
    serializer | entry._isExternal;
  }
};

struct TripleComponentWithId {
  std::string _iriOrLiteral;
  bool _isExternal = false;
  uint64_t _id = 0;

  [[nodiscard]] const auto& isExternal() const { return _isExternal; }
  [[nodiscard]] auto& isExternal() { return _isExternal; }
  [[nodiscard]] const auto& iriOrLiteral() const { return _iriOrLiteral; }
  [[nodiscard]] auto& iriOrLiteral() { return _iriOrLiteral; }

  template <typename Serializer>
  friend void serialize(Serializer& serializer, TripleComponentWithId& entry) {
    serializer | entry._iriOrLiteral;
    serializer | entry._isExternal;
    serializer | entry._id;
  }
};

// A triple that also knows for each entry, whether this entry should be
// part of the external vocabulary.
using Triple = std::array<TripleComponent, 3>;

// Convert a triple of `std::string` to a triple of `TripleComponents`. All
// three entries will have `isExternal()==false` and an uninitialized ID.
inline Triple makeTriple(std::array<std::string, 3>&& t) {
  using T = TripleComponent;
  return {T{t[0]}, T{t[1]}, T{t[2]}};
}

/// named value type for the ItemMap
struct IdAndSplitVal {
  Id m_id;
  TripleComponentComparator::SplitVal m_splitVal;
};

using ItemMap = ad_utility::HashMap<std::string, IdAndSplitVal>;
using ItemMapArray = std::array<ItemMap, NUM_PARALLEL_ITEM_MAPS>;
using ItemVec = std::vector<std::pair<std::string, IdAndSplitVal>>;

/**
 * Manage a HashMap of string->Id to create unique Ids for strings.
 * Ids are assigned in an adjacent range starting with a configurable
 * minimum Id. That way multiple maps can be used with non overlapping ranges.
 */
// Align each ItemMapManager on its own cache line to avoid false sharing.
struct alignas(256) ItemMapManager {
  /// Construct by assigning the minimum ID that should be returned by the map.
  explicit ItemMapManager(Id minId, const TripleComponentComparator* cmp)
      : _map(), _minId(minId), m_comp(cmp) {}
  /// Minimum Id is 0
  ItemMapManager() = default;

  /// Move the held HashMap out as soon as we are done inserting and only need
  /// the actual vocabulary
  ItemMap&& moveMap() && { return std::move(_map); }

  /// If the key was seen before, return its preassigned ID. Else assign the
  /// next free ID to the string, store and return it.
  Id getId(const TripleComponent& key) {
    if (!_map.count(key._iriOrLiteral)) {
      Id res = _map.size() + _minId;
      _map[key._iriOrLiteral] = {
          res,
          m_comp->extractAndTransformComparable(
              key._iriOrLiteral, TripleComponentComparator::Level::IDENTICAL,
              key._isExternal)};
      return res;
    } else {
      return _map[key._iriOrLiteral].m_id;
    }
  }

  /// call getId for each of the Triple elements.
  std::array<Id, 3> getId(const Triple& t) {
    return {getId(t[0]), getId(t[1]), getId(t[2])};
  }
  ItemMap _map;
  Id _minId = 0;
  const TripleComponentComparator* m_comp = nullptr;
};

/// Combines a triple (three strings) together with the (possibly empty)
/// language tag of its object.
struct LangtagAndTriple {
  std::string _langtag;
  Triple _triple;
};

/**
 * @brief Get the tuple of lambda functions that is needed for the String-> Id
 * step of the Index building Pipeline
 *
 * return a tuple of <Parallelism> lambda functions, each lambda does the
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
template <size_t Parallelism>
auto getIdMapLambdas(std::array<ItemMapManager, Parallelism>* itemArrayPtr,
                     size_t maxNumberOfTriples,
                     const TripleComponentComparator* comp) {
  // that way the different ids won't interfere
  auto& itemArray = *itemArrayPtr;
  for (size_t j = 0; j < Parallelism; ++j) {
    itemArray[j] = ItemMapManager(j * 100 * maxNumberOfTriples, comp);
    // The LANGUAGE_PREDICATE gets the first ID in each map. TODO<joka921>
    // This is not necessary for the actual QLever code, but certain unit tests
    // currently fail without it.
    itemArray[j].getId(LANGUAGE_PREDICATE);
    itemArray[j]._map.reserve(2 * maxNumberOfTriples);
  }
  using OptionalIds = std::array<std::optional<std::array<Id, 3>>, 3>;

  /* given an index idx, returns a lambda that
   * - Takes a triple and a language tag
   * - Returns OptionalIds where the first entry are the Ids for the triple,
   *   the second and third entry are the Ids of the extra triples for the
   *   language filter implementation (or std::nullopt if there was no language
   * tag)
   * - All Ids are assigned according to itemArray[idx]
   */
  const auto itemMapLamdaCreator = [&itemArray](const size_t idx) {
    return [&map = itemArray[idx]](LangtagAndTriple&& lt) {
      OptionalIds res;
      // get Ids for the actual triple and store them in the result.
      res[0] = map.getId(lt._triple);
      if (!lt._langtag.empty()) {  // the object of the triple was a literal
                                   // with a language tag
        // get the Id for the corresponding langtag Entity
        auto langTagId =
            map.getId(ad_utility::convertLangtagToEntityUri(lt._langtag));
        // get the Id for the tagged predicate, e.g. @en@rdfs:label
        auto langTaggedPredId =
            map.getId(ad_utility::convertToLanguageTaggedPredicate(
                lt._triple[1]._iriOrLiteral, lt._langtag));
        auto& spoIds = *(res[0]);  // ids of original triple
        // TODO replace the std::array by an explicit IdTriple class,
        //  then the emplace calls don't need the explicit type.
        // extra triple <subject> @language@<predicate> <object>
        res[1].emplace(
            std::array<Id, 3>{spoIds[0], langTaggedPredId, spoIds[2]});
        // extra triple <object> ql:language-tag <@language>
        res[2].emplace(std::array<Id, 3>{
            spoIds[2], map.getId(LANGUAGE_PREDICATE), langTagId});
      }
      return res;
    };
  };

  // setup a tuple with one lambda function per map in the itemArray
  // (the first lambda will assign ids according to itemArray[1]...
  auto itemMapLambdaTuple =
      ad_tuple_helpers::setupTupleFromCallable<Parallelism>(
          itemMapLamdaCreator);
  return itemMapLambdaTuple;
}
#endif  // QLEVER_INDEXBUILDERTYPES_H
