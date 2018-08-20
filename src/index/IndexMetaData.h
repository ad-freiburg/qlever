// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <utility>
#include <vector>

#include <stdio.h>
#include <algorithm>
#include <cmath>
#include "../global/Id.h"
#include "../util/File.h"
#include "../util/HashMap.h"
#include "../util/MmapVector.h"
#include "../util/ReadableNumberFact.h"
#include "./MetaDataHandler.h"
#include "./MetaDataTypes.h"

using std::array;
using std::pair;
using std::vector;

// Check index_layout.md for explanations (expected comments).
// Removed comments here so that not two places had to be kept up-to-date.

// Templated MetaData. The datatype wrappers defined in MetaDataHandler.h
// all meet the requirements of MapType
// TODO(C++20): When concepts are available, MapType is a Concept!
template <class MapType>
class IndexMetaData {
 public:
  // some MapTypes (the dense ones using stxxl or mmap) require additional calls
  // to setup() before being fully initialized
  IndexMetaData() = default;

  // pass all arguments that are needed for initialization to the underlying
  // implementation of MapType _data
  template <typename... dataArgs>
  void setup(dataArgs&&... args) {
    _data.setup(std::forward<dataArgs>(args)...);
  }

  // persistentRMD == true means we do not need to add rmd to _data
  // but assume that it is already contained in _data (for persistent
  // metaData implementations. Must be a compile time parameter because we have
  // to avoid instantation of member function set() for readonly MapTypes (e.g.
  // based on MmapVectorView
  template <bool persistentRMD = false>
  void add(const FullRelationMetaData& rmd,
           const BlockBasedRelationMetaData& bRmd);

  off_t getOffsetAfter() const;

  const RelationMetaData getRmd(Id relId) const;

  // Persistent meta data MapTypes (called MmapBased here) have to be separated
  // from RAM-based (e.g. hashMap based sparse) ones at compile time, this is
  // done in the following block
  using MetaWrapperMmap =
      MetaDataWrapperDense<ad_utility::MmapVector<FullRelationMetaData>>;
  using MetaWrapperMmapView =
      MetaDataWrapperDense<ad_utility::MmapVectorView<FullRelationMetaData>>;
  template <typename T>
  struct IsMmapBased {
    static const bool value = std::is_same<MetaWrapperMmap, T>::value ||
                              std::is_same<MetaWrapperMmapView, T>::value;
  };
  // compile time information whether this instatiation if MMapBased or not
  static constexpr bool _isMmapBased = IsMmapBased<MapType>::value;

  void createFromByteBuffer(unsigned char* buf) {
    if constexpr (_isMmapBased) {
      return createFromByteBufferMmap(buf);
    } else {
      return createFromByteBufferSparse(buf);
    }
  }

  // dispatched functions for createFromByteBuffer
  void createFromByteBufferMmap(unsigned char* buf);
  void createFromByteBufferSparse(unsigned char* buf);

  // Write to a file that will be overwritten/created
  void writeToFile(const std::string& filename) const;

  // Write to the end of an already existing file.
  // Will move file pointer to the end of the file
  void appendToFile(ad_utility::File* file) const;

  // read from a file at path specified by filename
  void readFromFile(const std::string& filename);

  // read from file that already must opened and has
  // valid metadata at its end
  void readFromFile(ad_utility::File* file);

  bool relationExists(Id relId) const;

  string statistics() const;

  size_t getNofTriples() const { return _nofTriples; }

  void setName(const string& name) { _name = name; }

  const string& getName() const { return _name; }

  size_t getNofDistinctC1() const;

 private:
  off_t _offsetAfter = 0;
  size_t _nofTriples;

  string _name;
  string _filename;

  MapType _data;
  ad_utility::HashMap<Id, BlockBasedRelationMetaData> _blockData;

  // friend declaration for external converter function with ugly types
  using IndexMetaDataHmap = IndexMetaData<MetaDataWrapperHashMap>;
  using IndexMetaDataMmap = IndexMetaData<
      MetaDataWrapperDense<ad_utility::MmapVector<FullRelationMetaData>>>;
  friend IndexMetaDataMmap convertHmapMetaDataToMmap(const IndexMetaDataHmap&,
                                                     const std::string&, bool);

  // this way all instantations will be friends with each other,
  // but this should not be an issue.
  template <class U>
  friend inline ad_utility::File& operator<<(ad_utility::File& f,
                                             const IndexMetaData<U>& rmd);

  size_t getNofBlocksForRelation(const Id relId) const;

  size_t getTotalBytesForRelation(const FullRelationMetaData& frmd) const;
};

// ____________________________________________________________________________
template <class MapType>
ad_utility::File& operator<<(ad_utility::File& f,
                             const IndexMetaData<MapType>& imd);

// aliases for easier use in Index class
using MetaWrapperMmap =
    MetaDataWrapperDense<ad_utility::MmapVector<FullRelationMetaData>>;
using MetaWrapperMmapView =
    MetaDataWrapperDense<ad_utility::MmapVectorView<FullRelationMetaData>>;
using IndexMetaDataHmap = IndexMetaData<MetaDataWrapperHashMap>;
using IndexMetaDataMmap = IndexMetaData<
    MetaDataWrapperDense<ad_utility::MmapVector<FullRelationMetaData>>>;
using IndexMetaDataMmapView = IndexMetaData<
    MetaDataWrapperDense<ad_utility::MmapVectorView<FullRelationMetaData>>>;

// constants for Magic Numbers to separate different types of MetaData;
const size_t MAGIC_NUMBER_MMAP_META_DATA = static_cast<size_t>(-1);
const size_t MAGIC_NUMBER_SPARSE_META_DATA = static_cast<size_t>(-2);

#include "./IndexMetaDataImpl.h"
