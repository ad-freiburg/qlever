// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <stdio.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <exception>
#include <limits>
#include <utility>
#include <vector>

#include "../global/Id.h"
#include "../util/File.h"
#include "../util/HashMap.h"
#include "../util/MmapVector.h"
#include "../util/ReadableNumberFact.h"
#include "./MetaDataHandler.h"
#include "CompressedRelation.h"

using std::array;
using std::pair;
using std::vector;

// an exception thrown when we want to construct Mmap meta data from Hmap meta
// data and vice versa
class WrongFormatException : public std::exception {
 public:
  WrongFormatException(std::string msg) : _msg(std::move(msg)) {}
  const char* what() const throw() { return _msg.c_str(); }

 private:
  std::string _msg;
};

// simple ReturnValue struct
struct VersionInfo {
  uint64_t _version;
  size_t _nOfBytes;
};

// read from a buffer and advance it
// helper function for IndexMetaData::createFromByteBuffer
template <class T>
T readFromBuf(unsigned char** buf) {
  T res = *reinterpret_cast<T*>(*buf);
  *buf += sizeof(T);
  return res;
}

// constants for Magic Numbers to separate different types of MetaData;
const uint64_t MAGIC_NUMBER_MMAP_META_DATA =
    std::numeric_limits<uint64_t>::max();
const uint64_t MAGIC_NUMBER_SPARSE_META_DATA =
    std::numeric_limits<uint64_t>::max() - 1;
const uint64_t MAGIC_NUMBER_MMAP_META_DATA_VERSION =
    std::numeric_limits<uint64_t>::max() - 2;
const uint64_t MAGIC_NUMBER_SPARSE_META_DATA_VERSION =
    std::numeric_limits<uint64_t>::max() - 3;

// constants for meta data versions in case the format is changed again
constexpr uint64_t V_NO_VERSION = 0;  // this is  a dummy
constexpr uint64_t V_BLOCK_LIST_AND_STATISTICS = 1;
constexpr uint64_t V_SERIALIZATION_LIBRARY = 2;

// this always tags the current version
constexpr uint64_t V_CURRENT = V_SERIALIZATION_LIBRARY;

// Check index_layout.md for explanations (expected comments).
// Removed comments here so that not two places had to be kept up-to-date.

// Templated MetaData. The datatype wrappers defined in MetaDataHandler.h
// all meet the requirements of MapType
// TODO(C++20): When concepts are available, MapType is a Concept!
template <class M>
class IndexMetaData {
 public:
  // This allows access to MapType given the type of IndexMetaData
  typedef M MapType;
  using value_type = typename MapType::value_type;

  using AddType = CompressedRelationMetaData;
  using GetType = const CompressedRelationMetaData&;
  using BlocksType = std::vector<CompressedBlockMetaData>;
  ;

  // some MapTypes (the dense ones using stxxl or mmap) require additional calls
  // to setup() before being fully initialized
  IndexMetaData() = default;

  // pass all arguments that are needed for initialization to the underlying
  // implementation of MapType _data
  template <typename... dataArgs>
  void setup(dataArgs&&... args) {
    _data.setup(std::forward<dataArgs>(args)...);
  }

  // isPersistentMetaData == true means we do not need to add rmd to _data
  // but assume that it is already contained in _data (for persistent
  // metaData implementations. Must be a compile time parameter because we have
  // to avoid instantation of member function set() for readonly MapTypes (e.g.
  // based on MmapVectorView
  template <bool isPersistentMetaData = false>
  void add(AddType addedValue);

  off_t getOffsetAfter() const;

  GetType getMetaData(Id col0Id) const;

  // Persistent meta data MapTypes (called MmapBased here) have to be separated
  // from RAM-based (e.g. hashMap based sparse) ones at compile time, this is
  // done in the following block
  using MetaWrapperMmap =
      MetaDataWrapperDense<ad_utility::MmapVector<CompressedRelationMetaData>>;
  using MetaWrapperMmapView = MetaDataWrapperDense<
      ad_utility::MmapVectorView<CompressedRelationMetaData>>;
  template <typename T>
  struct IsMmapBased {
    static const bool value = std::is_same<MetaWrapperMmap, T>::value ||
                              std::is_same<MetaWrapperMmapView, T>::value;
  };
  // Compile time information whether this instatiation if MMapBased or not
  static constexpr bool _isMmapBased = IsMmapBased<MapType>::value;

  // This magic number is written when serializing the IndexMetaData to a file.
  // It is used to check, whether this is a really old index that requires
  // rebuilding.
  static constexpr uint64_t MAGIC_NUMBER_FOR_SERIALIZATION =
      _isMmapBased ? MAGIC_NUMBER_MMAP_META_DATA_VERSION
                   : MAGIC_NUMBER_SPARSE_META_DATA_VERSION;

  // Write to a file that will be overwritten/created
  void writeToFile(const std::string& filename) const;

  // Write to the end of an already existing file.
  // Will move file pointer to the end of the file
  void appendToFile(ad_utility::File* file) const;

  // read from a file at path specified by filename
  void readFromFile(const std::string& filename);

  // read from file that already must opened and has
  // valid metadata at its end. Will move the seek pointer
  // of file.
  void readFromFile(ad_utility::File* file);

  bool col0IdExists(Id col0Id) const;

  // calculate and save statistics that are expensive to calculate so we only
  // have to do this during the index build and not at server startup
  void calculateExpensiveStatistics();
  string statistics() const;

  size_t getNofTriples() const { return _totalElements; }

  void setName(const string& name) { _name = name; }

  const string& getName() const { return _name; }

  size_t getNofDistinctC1() const;

  size_t getVersion() const { return _version; }

  MapType& data() { return _data; }
  const MapType& data() const { return _data; }

  BlocksType& blockData() { return _blockData; }
  const BlocksType& blockData() const { return _blockData; }

 private:
  off_t _offsetAfter = 0;

  string _name;
  string _filename;

  MapType _data;
  BlocksType _blockData;
  size_t _totalElements = 0;
  size_t _totalBytes = 0;
  size_t _totalBlocks = 0;
  uint64_t _version = V_CURRENT;

  // Symmetric serialization function for the ad_utility::serialization module.
  template <class Serializer, typename MapType>
  friend void serialize(Serializer& serializer,
                        IndexMetaData<MapType>& metaData);

  size_t getNofBlocksForRelation(const Id col0Id) const;

  size_t getTotalBytesForRelation(Id col0Id) const;
};

// ____________________________________________________________________________
template <class MapType>
ad_utility::File& operator<<(ad_utility::File& f,
                             const IndexMetaData<MapType>& imd);

// aliases for easier use in Index class
using MetaWrapperMmap =
    MetaDataWrapperDense<ad_utility::MmapVector<CompressedRelationMetaData>>;
using MetaWrapperMmapView = MetaDataWrapperDense<
    ad_utility::MmapVectorView<CompressedRelationMetaData>>;
using MetaWrapperHashMap =
    MetaDataWrapperHashMap<ad_utility::HashMap<Id, CompressedRelationMetaData>>;
using IndexMetaDataHmap = IndexMetaData<MetaWrapperHashMap>;
using IndexMetaDataMmap = IndexMetaData<MetaWrapperMmap>;
using IndexMetaDataMmapView = IndexMetaData<MetaWrapperMmapView>;

#include "./IndexMetaDataImpl.h"
