//
// Created by johannes on 26.01.20.
//
// Common classes / Typedefs that are used during Index Creation

#include "../global/Constants.h"
#include "../global/Id.h"
#include "../util/Conversions.h"
#include "../util/HashMap.h"
#include "../util/TupleHelpers.h"
#include "./ConstantsIndexCreation.h"
#include "./StringSortComparator.h"

#ifndef QLEVER_INDEXBUILDERTYPES_H
#define QLEVER_INDEXBUILDERTYPES_H

using Triple = std::array<std::string, 3>;

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
struct ItemMapManager {
  /// Construct by assigning the minimum Id that shall be returned by the map
  explicit ItemMapManager(Id minId, const TripleComponentComparator* cmp) : _map(), _minId(minId), m_comp(cmp) {}
  /// Minimum Id is 0
  ItemMapManager() = default;

  /// Move the held HashMap out as soon as we are done inserting and only need
  /// the actual vocabulary
  ItemMap&& moveMap() && { return std::move(_map); }

  /// If the key was seen before, return its preassigned Id. Else assign the
  /// Next free Id to the string, store and return it.
  Id assignNextId(const string& key) {
    if (!_map.count(key)) {
      Id res = _map.size() + _minId;
      _map[key] = {res, m_comp->extractAndTransformComparable(key, TripleComponentComparator::Level::IDENTICAL)};
      return res;
    } else {
      return _map[key].m_id;
    }
  }

  /// call assignNextId for each of the Triple elements.
  std::array<Id, 3> assignNextId(const Triple& t) {
    return {assignNextId(t[0]), assignNextId(t[1]), assignNextId(t[2])};
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
                     size_t maxNumberOfTriples, const TripleComponentComparator* comp) {
  // that way the different ids won't interfere
  auto& itemArray = *itemArrayPtr;
  for (size_t j = 0; j < Parallelism; ++j) {
    itemArray[j] = ItemMapManager(j * 100 * maxNumberOfTriples, comp);
    itemArray[j].assignNextId(
        LANGUAGE_PREDICATE);  // not really needed here, but also not harmful
                              // and needed to make some completely unrelated
                              // unit tests pass.
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
      res[0] = map.assignNextId(lt._triple);
      if (!lt._langtag.empty()) {  // the object of the triple was a literal
                                   // with a language tag
        // get the Id for the corresponding langtag Entity
        auto langTagId = map.assignNextId(
            ad_utility::convertLangtagToEntityUri(lt._langtag));
        // get the Id for the tagged predicate, e.g. @en@rdfs:label
        auto langTaggedPredId =
            map.assignNextId(ad_utility::convertToLanguageTaggedPredicate(
                lt._triple[1], lt._langtag));
        auto& spoIds = *(res[0]);  // ids of original triple
        // extra triple <subject> @language@<predicate> <object>
        res[1].emplace(array<Id, 3>{spoIds[0], langTaggedPredId, spoIds[2]});
        // extra triple <object> ql:language-tag <@language>
        res[2].emplace(array<Id, 3>{
            spoIds[2], map.assignNextId(LANGUAGE_PREDICATE), langTagId});
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
