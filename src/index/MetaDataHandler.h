// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)
//
#pragma once

#include <cassert>
#include <stxxl/vector>

#include "../global/Id.h"
#include "../util/Exception.h"
#include "../util/HashMap.h"
#include "../util/Log.h"
#include "./MetaDataTypes.h"

// _____________________________________________________________
// hidden in implementation namespace
namespace VecWrapperImpl {
template <class Vec>
// iterator class for a unordered_map interface based on an array with the size
// of the key space. access is done by the index, and when iterating, we have
// to skip the empty entries. Needs an empty key to work properly
class Iterator {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = std::pair<Id, FullRelationMetaData>;

  // _________________________________________________
  std::pair<Id, const FullRelationMetaData&> operator*() const {
    // make sure that we do not conflict with the empty key
    AD_CHECK(_id != size_t(-1));
    return std::make_pair(_id, std::cref(*_it));
  }

  // _________________________________________________
  std::pair<Id, std::reference_wrapper<const FullRelationMetaData>>*
  operator->() const {
    // make sure that we do not conflict with the empty key
    AD_CHECK(_id != size_t(-1));
    _accessPair = **this;
    return &_accessPair;
  }

  // _____________________________________________________________
  Iterator(Id id, const typename Vec::const_iterator& it, const Vec* const vec)
      : _id(id), _it(it), _accessPair(**this), _vec(vec) {}

  // ________________________________________________
  Iterator& operator++() {
    ++_id;
    ++_it;
    goToNexValidEntry();

    return *this;
  }

  // ___________________________________________________
  void goToNexValidEntry() {
    while (_it != _vec->cend() && (*_it) == emptyMetaData) {
      ++_id;
      ++_it;
    }
  }

  // ________________________________________________
  Iterator operator++(int) {
    Iterator old(*this);
    ++_id;
    ++_it;
    goToNexValidEntry();
    return old;
  }

  // _______________________________________________
  bool operator==(const Iterator& other) const { return _it == other._it; }

  // _______________________________________________
  bool operator!=(const Iterator& other) const { return _it != other._it; }

 private:
  Id _id;
  typename Vec::const_iterator _it;
  // here we store the pair needed for operator->()
  // will be updated before each access
  mutable std::pair<Id, std::reference_wrapper<const FullRelationMetaData>>
      _accessPair;
  const Vec* const _vec;

  const FullRelationMetaData emptyMetaData = FullRelationMetaData::empty;
};
}  // namespace VecWrapperImpl

// _____________________________________________________________________
template <class M>
class MetaDataWrapperDense {
 public:
  using Iterator = VecWrapperImpl::Iterator<M>;

  // _________________________________________________________
  MetaDataWrapperDense() = default;

  // ________________________________________________________
  MetaDataWrapperDense(MetaDataWrapperDense<M>&& other)
      : _size(other._size), _vec(std::move(other._vec)) {}

  // ______________________________________________________________
  MetaDataWrapperDense& operator=(MetaDataWrapperDense<M>&& other) {
    _size = other._size;
    _vec = std::move(other._vec);
    return *this;
  }

  // Templated setup version
  // Arguments are passsed through to template argument M.
  // TODO<joka921>: enable_if  for better error messages
  template <typename... Args>
  void setup(Args... args) {
    // size has to be set correctly by a call to setSize(), this is done
    // in IndexMetaData::createFromByteBuffer
    _size = 0;
    _vec = M(args...);
  }

  // ___________________________________________________________
  size_t size() const { return _size; }

  // ___________________________________________________________
  void setSize(size_t newSize) { _size = newSize; }

  // __________________________________________________________________
  Iterator cbegin() const {
    Iterator it(0, _vec.cbegin(), &_vec);
    it.goToNexValidEntry();
    return it;
  }

  // __________________________________________________________________
  Iterator begin() const {
    Iterator it(0, _vec.begin(), &_vec);
    it.goToNexValidEntry();
    return it;
  }

  // __________________________________________________________________________
  Iterator cend() const { return Iterator(_vec.size(), _vec.cend(), &_vec); }

  // __________________________________________________________________________
  Iterator end() const { return Iterator(_vec.size(), _vec.end(), &_vec); }

  // ____________________________________________________________
  void set(Id id, const FullRelationMetaData& value) {
    if (id >= _vec.size()) {
      AD_CHECK(id < _vec.size());
    }
    bool previouslyEmpty = _vec[id] == emptyMetaData;

    // check that we never insert the empty key
    assert(value != emptyMetaData);
    _vec[id] = value;
    if (previouslyEmpty) {
      _size++;
    }
  }

  // __________________________________________________________
  const FullRelationMetaData& getAsserted(Id id) const {
    const auto& res = _vec[id];
    AD_CHECK(res != emptyMetaData);
    return res;
  }

  // _________________________________________________________
  FullRelationMetaData& operator[](Id id) {
    auto& res = _vec[id];
    AD_CHECK(res != emptyMetaData);
    return res;
  }

  // ________________________________________________________
  size_t count(Id id) const {
    // can either be 1 or 0 for map-like types
    return _vec[id] != emptyMetaData;
  }

  // ___________________________________________________________
  std::string getFilename() const { return _vec.getFilename(); }

 private:
  // the empty key, must be the first member to be initialized
  const FullRelationMetaData emptyMetaData = FullRelationMetaData::empty;
  size_t _size = 0;
  M _vec;
};

// _____________________________________________________________________
template <class hashMap>
class MetaDataWrapperHashMap {
 public:
  // using hashMap = ad_utility::HashMap<Id, FullRelationMetaData>;
  // using hashMap = ad_utility::HashMap<Id, FullRelationMetaData>;
  using ConstIterator = typename hashMap::const_iterator;
  using Iterator = typename hashMap::iterator;

  // nothing to do here, since the default constructor of the hashMap does
  // everything we want
  explicit MetaDataWrapperHashMap() = default;
  // nothing to setup, but has  to be defined to meet template requirements
  void setup(){};

  // _______________________________________________________________
  size_t size() const { return _map.size(); }

  // __________________________________________________________________
  ConstIterator cbegin() const { return _map.begin(); }

  // __________________________________________________________________
  Iterator begin() { return _map.begin(); }

  // ____________________________________________________________
  ConstIterator cend() const { return _map.end(); }

  // ____________________________________________________________
  Iterator end() { return _map.end(); }

  // ____________________________________________________________
  void set(Id id, const FullRelationMetaData& value) { _map[id] = value; }

  // __________________________________________________________
  const FullRelationMetaData& getAsserted(Id id) const {
    auto it = _map.find(id);
    AD_CHECK(it != _map.end());
    return std::cref(it->second);
  }

  // __________________________________________________________
  FullRelationMetaData& operator[](Id id) {
    auto it = _map.find(id);
    AD_CHECK(it != _map.end());
    return std::ref(it->second);
  }

  // ________________________________________________________
  size_t count(Id id) const {
    // can either be 1 or 0 for map-like types
    return _map.count(id);
  }

 private:
  hashMap _map;
};
