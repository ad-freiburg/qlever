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
#include "../util/Iterators.h"
#include "../util/Log.h"
#include "../util/Serializer/Serializer.h"
#include "./CompressedRelation.h"

// _____________________________________________________________________
template <class M>
class MetaDataWrapperDense {
 public:
  template <typename BaseIterator>
  struct AddGetIdIterator : BaseIterator {
    using BaseIterator::BaseIterator;
    AddGetIdIterator(BaseIterator base) : BaseIterator{base} {}
    [[nodiscard]] Id getId() const { return getIdFromElement(*(*this)); }
    static Id getIdFromElement(const typename BaseIterator::value_type& v) {
      return v.col0Id_;
    }
    static auto getNumRowsFromElement(
        const typename BaseIterator::value_type& v) {
      return v.numRows_;
    }
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
  AD_SERIALIZE_FRIEND_FUNCTION(MetaDataWrapperDense) {
    (void)serializer;
    (void)arg;
  }

  // ___________________________________________________________
  size_t size() const { return _vec.size(); }

  // __________________________________________________________________
  ConstIterator cbegin() const { return _vec.begin(); }

  // __________________________________________________________________
  ConstIterator begin() const { return _vec.begin(); }

  // __________________________________________________________________________
  ConstOrderedIterator ordered_begin() const { return begin(); }

  // __________________________________________________________________________
  ConstIterator cend() const { return _vec.end(); }

  // __________________________________________________________________________
  ConstIterator end() const { return _vec.end(); }

  // __________________________________________________________________________
  ConstOrderedIterator ordered_end() const { return end(); }

  // ____________________________________________________________
  void set(Id id, const value_type& value) {
    // Assert that the ids are ascending.
    AD_CONTRACT_CHECK(_vec.size() == 0 || _vec.back().col0Id_ < id);
    _vec.push_back(value);
  }

  // __________________________________________________________
  const value_type& getAsserted(Id id) const {
    auto it = lower_bound(id);
    AD_CONTRACT_CHECK(it != _vec.end() && it->col0Id_ == id);
    return *it;
  }

  // ________________________________________________________
  size_t count(Id id) const {
    auto it = lower_bound(id);
    return it != _vec.end() && it->col0Id_ == id;
  }

  // ___________________________________________________________
  std::string getFilename() const { return _vec.getFilename(); }

 private:
  ConstIterator lower_bound(Id id) const {
    auto cmp = [](const auto& metaData, Id id) {
      return metaData.col0Id_ < id;
    };
    return std::lower_bound(_vec.begin(), _vec.end(), id, cmp);
  }
  M _vec;
};
