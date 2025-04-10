// Copyright 2018 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_METADATAHANDLER_H
#define QLEVER_SRC_INDEX_METADATAHANDLER_H

#include <string>

#include "global/Id.h"
#include "util/Exception.h"
#include "util/Serializer/Serializer.h"

// Wrapper class for access to `CompressedRelationMetadata` objects (one per
// relation) stored in a vector. Specifically, our index uses this with `M =
// MmapVector<CompressedRelationMetadata>>`; see `index/IndexMetaData.h` at the
// bottom.
//
// TODO: We needed this at some point because we used to have two implementation
// of `IndexMetaData`, one using mmaps and one using hash maps, and we wanted to
// have a common interface for both. We no longer use the hash map
// implementation and so the wrapper class (and the complexity that goes along
// with it) is probably no longer needed.
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

  // The type of the stored metadata objects.
  using value_type = typename M::value_type;

  // _________________________________________________________
  MetaDataWrapperDense() = default;

  // ________________________________________________________
  MetaDataWrapperDense(MetaDataWrapperDense<M>&& other) = default;

  // ______________________________________________________________
  MetaDataWrapperDense& operator=(MetaDataWrapperDense<M>&& other) = default;

  // Templated setup version
  // Arguments are passed through to template argument M.
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
    // Check that the `Id`s are added in strictly ascending order.
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
  Iterator lower_bound(Id id) {
    auto cmp = [](const auto& metaData, Id id) {
      return metaData.col0Id_ < id;
    };
    return std::lower_bound(_vec.begin(), _vec.end(), id, cmp);
  }
};

#endif  // QLEVER_SRC_INDEX_METADATAHANDLER_H
