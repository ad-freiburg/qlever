// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)
//
#pragma once

#include <cassert>
#include <stxxl/vector>

#include "../global/Id.h"
#include "../util/Exception.h"
#include "../util/HashMap.h"
#include "../util/Log.h"
#include "./CompressedRelation.h"

// _____________________________________________________________________
template <class M>
class MetaDataWrapperDense {
 public:
  template <typename BaseIterator>
  struct AddGetIdIterator : BaseIterator {
    using BaseIterator::BaseIterator;
    AddGetIdIterator(BaseIterator base) : BaseIterator{base} {}
    uint64_t getId() const { return (*this)->_col0Id; }
  };

  using Iterator = AddGetIdIterator<typename M::iterator>;
  using ConstIterator = AddGetIdIterator<typename M::const_iterator>;

  // The underlying array is sorted, so all iterators are ordered iterators
  using ConstOrderedIterator = ConstIterator;

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
  template <typename Serializer>
  friend void serialize([[maybe_unused]] Serializer& serializer,
                        [[maybe_unused]] MetaDataWrapperDense& wrapper) {}

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
    AD_CHECK(_vec.size() == 0 || _vec.back()._col0Id < id);
    _vec.push_back(value);
  }

  // __________________________________________________________
  const value_type& getAsserted(Id id) const {
    auto it = lower_bound(id);
    AD_CHECK(it != _vec.end() && it->_col0Id == id);
    return *it;
  }

  // _________________________________________________________
  value_type& operator[](Id id) {
    auto it = lower_bound(id);
    AD_CHECK(it != _vec.end() && it->_col0Id == id);
    return *it;
  }

  // ________________________________________________________
  size_t count(Id id) const {
    auto it = lower_bound(id);
    return it != _vec.end() && it->_col0Id == id;
  }

  // ___________________________________________________________
  std::string getFilename() const { return _vec.getFilename(); }

 private:
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
  M _vec;
};

// _____________________________________________________________________
template <class hashMap>
class MetaDataWrapperHashMap {
 public:
  template <typename BaseIterator>
  struct AddGetIdIterator : public BaseIterator {
    using BaseIterator::BaseIterator;
    AddGetIdIterator(BaseIterator base) : BaseIterator{base} {}
    uint64_t getId() const { return (*this)->first; }
  };
  using Iterator = AddGetIdIterator<typename hashMap::iterator>;
  using ConstIterator = AddGetIdIterator<typename hashMap::const_iterator>;

  using value_type = typename hashMap::mapped_type;

  // An iterator on the underlying hashMap that iterates over the elements
  // in order. This is used for deterministically exporting the underlying
  // permutation.
  class ConstOrderedIterator {
    using key_type = typename hashMap::key_type;

    const MetaDataWrapperHashMap& wrapper_;
    std::vector<key_type> sortedKeys_;
    size_t position_;

   public:
    // ________________________________________________________________________
    ConstOrderedIterator(const MetaDataWrapperHashMap& wrapper, size_t position)
        : wrapper_{wrapper}, position_{position} {
      // Sort all the keys from the underlying hashMap and store them.
      sortedKeys_.reserve(wrapper.size());
      for (const auto& [key, value] : wrapper_) {
        (void)value;  // Silence the warning about `value` being unused.
        sortedKeys_.push_back(key);
      }
      std::sort(sortedKeys_.begin(), sortedKeys_.end());
    }

    // ________________________________________________________________________
    const auto& operator*() const {
      const auto& m = wrapper_.getUnderlyingHashMap();
      return *m.find(sortedKeys_[position_]);
    }

    // _________________________________________________
    const auto* operator->() const {
      // Call operator* and return a pointer to the result.
      // This is safe, because the underlying hashMap ensures the lifetime of
      // the returned reference;
      return &(**this);
    }

    // ________________________________________________________________________
    ConstOrderedIterator& operator++() {
      ++position_;
      return *this;
    }

    // _________________________________________________________________________
    ConstOrderedIterator operator++(int) {
      auto cpy = *this;
      ++position_;
      return cpy;
    }

    bool operator==(const ConstOrderedIterator& rhs) const {
      return position_ == rhs.position_;
    }

    // ________________________________________________________________________
    uint64_t getId() const { return (*this)->first; }
  };

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
    return ConstOrderedIterator{*this, 0};
  }

  // ____________________________________________________________
  ConstIterator cend() const { return _map.end(); }
  ConstIterator end() const { return _map.end(); }

  // ____________________________________________________________
  Iterator end() { return _map.end(); }

  // _________________________________________________________________________
  ConstOrderedIterator ordered_end() const {
    return ConstOrderedIterator{*this, size()};
  }

  // ____________________________________________________________
  void set(Id id, value_type value) { _map[id] = std::move(value); }

  const auto& getUnderlyingHashMap() const { return _map; }

  // __________________________________________________________
  const value_type& getAsserted(Id id) const {
    auto it = _map.find(id);
    AD_CHECK(it != _map.end());
    return std::cref(it->second);
  }

  // __________________________________________________________
  value_type& operator[](Id id) {
    auto it = _map.find(id);
    AD_CHECK(it != _map.end());
    return std::ref(it->second);
  }

  // ________________________________________________________
  size_t count(Id id) const {
    // can either be 1 or 0 for map-like types
    return _map.count(id);
  }

  template <typename Serializer>
  friend void serialize(Serializer& serializer,
                        MetaDataWrapperHashMap& metaDataWrapper) {
    serializer | metaDataWrapper._map;
  }

 private:
  hashMap _map;
};
