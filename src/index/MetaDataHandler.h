// Copyright 2018 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <cassert>
#include <stxxl/vector>

#include "global/Id.h"
#include "index/CompressedRelation.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/Serializer/Serializer.h"

// Class for access to relation metadata stored in a vector. Specifically, our
// index uses this with `M = MmapVector<CompressedRelationMetadata>>`; see
// `index/IndexMetaData.h`
template <class M>
class MetaDataWrapperDense {
 private:
  // A vector of metadata objects.
  M _vec;

 public:
  // An iterator with an additional method `getId()` that gives the relation ID
  // of the current metadata object.
  template <typename BaseIterator>
  struct AddGetIdIterator : BaseIterator {
    using BaseIterator::BaseIterator;
    AddGetIdIterator(BaseIterator base) : BaseIterator{base} {}
    [[nodiscard]] Id getId() const { return getIdFromElement(*(*this)); }
    [[nodiscard]] const auto& getMetaData() const { return *(*this); }
    static Id getIdFromElement(const typename BaseIterator::value_type& v) {
      return v._col0Id;
    }
    static auto getNumRowsFromElement(
        const typename BaseIterator::value_type& v) {
      return v._numRows;
    }
  };

  using Iterator = AddGetIdIterator<typename M::iterator>;
  using ConstIterator = AddGetIdIterator<typename M::const_iterator>;

  // The underlying array is sorted, so all iterators are ordered iterators
  using ConstOrderedIterator = ConstIterator;

  // The type of the stored metadata objects.
  using value_type = typename M::value_type;

  // _________________________________________________________
  MetaDataWrapperDense() = default;

  // ________________________________________________________
  MetaDataWrapperDense(MetaDataWrapperDense<M>&& other) = default;

  // ______________________________________________________________
  MetaDataWrapperDense& operator=(MetaDataWrapperDense<M>&& other) = default;

  // Templated setup version
  // Arguments are passsed through to template argument M.
  // TODO<joka921>: enable_if  for better error messages
  template <typename... Args>
  void setup(Args... args) {
    _vec = M(args...);
  }

  // The serialization is a noop because all the data always stays on disk.
  // The initialization is done via `setup`.
  AD_SERIALIZE_FRIEND_FUNCTION(MetaDataWrapperDense) {
    (void)serializer;
    (void)arg;
  }

  // ___________________________________________________________
  size_t size() const { return _vec.size(); }

  // __________________________________________________________________
  ConstIterator cbegin() const { return _vec.begin(); }

  // __________________________________________________________________
  Iterator begin() { return _vec.begin(); }

  // __________________________________________________________________
  ConstIterator begin() const { return _vec.begin(); }

  // __________________________________________________________________________
  ConstOrderedIterator ordered_begin() const { return begin(); }

  // __________________________________________________________________________
  ConstIterator cend() const { return _vec.end(); }

  // __________________________________________________________________________
  ConstIterator end() const { return _vec.end(); }
  Iterator end() { return _vec.end(); }

  // __________________________________________________________________________
  ConstOrderedIterator ordered_end() const { return end(); }

  // ____________________________________________________________
  void set(Id id, const value_type& value) {
    // Assert that the ids are ascending.
    AD_CONTRACT_CHECK(_vec.size() == 0 || _vec.back()._col0Id < id);
    _vec.push_back(value);
  }

  // __________________________________________________________
  const value_type& getAsserted(Id id) const {
    auto it = lower_bound(id);
    AD_CONTRACT_CHECK(it != _vec.end() && it->_col0Id == id);
    return *it;
  }

  // _________________________________________________________
  value_type& operator[](Id id) {
    auto it = lower_bound(id);
    AD_CONTRACT_CHECK(it != _vec.end() && it->_col0Id == id);
    return *it;
  }

  // ________________________________________________________
  size_t count(Id id) const {
    auto it = lower_bound(id);
    return it != _vec.end() && it->_col0Id == id;
  }

  // ___________________________________________________________
  std::string getFilename() const { return _vec.getFilename(); }

  // The following used to be private (because they were only used as
  // subroutines in the above), but we now need them in
  // `DeltaTriples::findTripleResult`.
  ConstIterator lower_bound(Id id) const {
    auto cmp = [](const auto& metaData, Id id) {
      return metaData._col0Id < id;
    };
    return std::lower_bound(_vec.begin(), _vec.end(), id, cmp);
  }
  Iterator lower_bound(Id id) {
    auto cmp = [](const auto& metaData, Id id) {
      return metaData._col0Id < id;
    };
    return std::lower_bound(_vec.begin(), _vec.end(), id, cmp);
  }
};

// Class for access to relation metadata stored in a hash map. Specifically, our
// index uses this with `M = HashMap<Id, CompressedRelationMetadata>>`; see
// `index/IndexMetaData.h`
template <class hashMap>
class MetaDataWrapperHashMap {
 private:
  // The map that maps each existing relation ID to its metadata object.
  hashMap _map;

  // The relation IDs in sorted order. This is only computed by the `serialize`
  // function defined by `AD_SERIALIZE_FRIEND_FUNCTION` below.
  std::vector<typename hashMap::key_type> _sortedKeys;

 public:
  // An iterator with an additional method `getId()` that gives the relation ID
  // of the current metadata object.
  template <typename BaseIterator>
  struct AddGetIdIterator : public BaseIterator {
    using BaseIterator::BaseIterator;
    AddGetIdIterator(BaseIterator base) : BaseIterator{base} {}
    [[nodiscard]] Id getId() const { return (*this)->second._col0Id; }
    [[nodiscard]] const auto& getMetaData() const { return (*this)->second; }
    static Id getIdFromElement(const typename BaseIterator::value_type& v) {
      return v.second._col0Id;
    }
    static auto getNumRowsFromElement(
        const typename BaseIterator::value_type& v) {
      return v.second._numRows;
    }
  };
  using Iterator = AddGetIdIterator<typename hashMap::iterator>;
  using ConstIterator = AddGetIdIterator<typename hashMap::const_iterator>;

  using value_type = typename hashMap::mapped_type;

  // An iterator on the underlying hashMap that iterates over the elements
  // in order. This is used for deterministically exporting the underlying
  // permutation.
  static inline auto getSortedKey = [](const auto& wrapper,
                                       uint64_t i) -> decltype(auto) {
    const auto& m = wrapper.getUnderlyingHashMap();
    return *m.find(wrapper.sortedKeys()[i]);
  };

  using ConstOrderedIteratorBase =
      ad_utility::IteratorForAccessOperator<MetaDataWrapperHashMap,
                                            decltype(getSortedKey)>;
  using ConstOrderedIterator = AddGetIdIterator<ConstOrderedIteratorBase>;

  // nothing to do here, since the default constructor of the hashMap does
  // everything we want
  explicit MetaDataWrapperHashMap() = default;
  // nothing to setup, but has  to be defined to meet template requirements
  void setup(){};

  // _______________________________________________________________
  size_t size() const { return _map.size(); }

  // __________________________________________________________________
  ConstIterator cbegin() const { return _map.begin(); }
  ConstIterator begin() const { return _map.begin(); }

  // __________________________________________________________________
  Iterator begin() { return _map.begin(); }

  // _________________________________________________________________________
  ConstOrderedIterator ordered_begin() const {
    return ConstOrderedIterator{this, 0};
  }

  // ____________________________________________________________
  ConstIterator cend() const { return _map.end(); }
  ConstIterator end() const { return _map.end(); }

  // ____________________________________________________________
  Iterator end() { return _map.end(); }

  // _________________________________________________________________________
  ConstOrderedIterator ordered_end() const {
    AD_CONTRACT_CHECK(size() == _sortedKeys.size());
    return ConstOrderedIterator{this, size()};
  }

  // ____________________________________________________________
  void set(Id id, value_type value) {
    _map[id] = std::move(value);
    if (!_sortedKeys.empty()) {
      AD_CONTRACT_CHECK(id > _sortedKeys.back());
    }
    _sortedKeys.push_back(id);
  }

  const auto& getUnderlyingHashMap() const { return _map; }

  // __________________________________________________________
  const value_type& getAsserted(Id id) const {
    auto it = _map.find(id);
    AD_CONTRACT_CHECK(it != _map.end());
    return std::cref(it->second);
  }

  // __________________________________________________________
  value_type& operator[](Id id) {
    auto it = _map.find(id);
    AD_CONTRACT_CHECK(it != _map.end());
    return std::ref(it->second);
  }

  // ________________________________________________________
  size_t count(Id id) const {
    // can either be 1 or 0 for map-like types
    return _map.count(id);
  }

  // Defines a friend method `serialize`, see the macro definition for details.
  AD_SERIALIZE_FRIEND_FUNCTION(MetaDataWrapperHashMap) {
    serializer | arg._map;
    if constexpr (ad_utility::serialization::ReadSerializer<S>) {
      arg._sortedKeys.clear();
      arg._sortedKeys.reserve(arg.size());
      for (const auto& [key, value] : arg) {
        (void)value;  // Silence the warning about `value` being unused.
        arg._sortedKeys.push_back(key);
      }
      std::sort(arg._sortedKeys.begin(), arg._sortedKeys.end());
    }
  }

  const auto& sortedKeys() const { return _sortedKeys; }

  // The following are needed in `DeltaTriples::findTripleResult`, so that we
  // have a common interface for all permutations. The functionality is as
  // follows:
  //
  // If a metadata object with the given `id` exists, return it (or rather, an
  // iterator to the corresponding key-value pair in the hash map).  If it is
  // not contained, return `end()`.
  //
  // This makes sense because this class is only used for storing the metadata
  // of the POS and PSO permutations. If we search an ID of a predicate that
  // does not exist in the index, it will get an ID from the local vocab, which
  // is larger than all existing IDs.
  //
  // TODO: This is not quite correct. We might insert a triple where the
  // predicate has an ID that exists in the original index, but it just hasn't
  // been used for a predicate in the original index. Then `end()` is not the
  // right answer when searching for that triple, but we indeed need the sorted
  // sequence of IDs (solution: just use an ordered `std::map` instead of an
  // unordered `HashMap`).
  //
  // (Note that this is different for OPS and OSP, because objects can also be
  // values and values that did not previously exist in the index get an
  // ID that can be properly compared with existing IDs. But that works find
  // because we store the metadata for OPS and OSP, as well as for SOP and SPO,
  // using a vector, see the class `MetaDataWrapperDense` above.)
  ConstIterator lower_bound(Id id) const { return _map.find(id); }
  Iterator lower_bound(Id id) { return _map.find(id); }
};
