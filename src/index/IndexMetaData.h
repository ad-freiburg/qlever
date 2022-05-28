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
#include "../util/Serializer/Serializer.h"
#include "./MetaDataHandler.h"
#include "CompressedRelation.h"

using std::array;
using std::pair;
using std::vector;

// An exception is thrown when we want to construct mmap meta data from hmap
// meta data or vice versa.
class WrongFormatException : public std::exception {
 public:
  WrongFormatException(std::string msg) : _msg(std::move(msg)) {}
  const char* what() const throw() { return _msg.c_str(); }

 private:
  std::string _msg;
};

// Struct so that we can return this in a single variable.
struct VersionInfo {
  uint64_t _version;
  size_t _nOfBytes;
};

// Magic numbers to separate different types of meta data.
const uint64_t MAGIC_NUMBER_MMAP_META_DATA =
    std::numeric_limits<uint64_t>::max();
const uint64_t MAGIC_NUMBER_SPARSE_META_DATA =
    std::numeric_limits<uint64_t>::max() - 1;
const uint64_t MAGIC_NUMBER_MMAP_META_DATA_VERSION =
    std::numeric_limits<uint64_t>::max() - 2;
const uint64_t MAGIC_NUMBER_SPARSE_META_DATA_VERSION =
    std::numeric_limits<uint64_t>::max() - 3;

// Constants for meta data version to keep the different versions apart.
constexpr uint64_t V_NO_VERSION = 0;  // this is  a dummy
constexpr uint64_t V_BLOCK_LIST_AND_STATISTICS = 1;
constexpr uint64_t V_SERIALIZATION_LIBRARY = 2;

// Constant for the current version.
constexpr uint64_t V_CURRENT = V_SERIALIZATION_LIBRARY;

// The meta data for an index permutation.
//
// TODO<C++20>: The datatype wrappers defined in MetaDataHandler.h all meet the
// requirements of MapType. Write this down using a C++20 concept.
template <class M>
class IndexMetaData {
  // Type definitions.
 public:
  typedef M MapType;
  using value_type = typename MapType::value_type;
  using AddType = CompressedRelationMetaData;
  using GetType = const CompressedRelationMetaData&;
  using BlocksType = std::vector<CompressedBlockMetaData>;

  // Private member variables.
 private:
  off_t _offsetAfter = 0;

  string _name;
  string _filename;

  // TODO: For each of the following two (_data and _blockData), both the type
  // name and the variable name are terrible.

  // For each relation, its meta data.
  MapType _data;
  // For each compressed block, its meta data.
  BlocksType _blockData;

  size_t _totalElements = 0;
  size_t _totalBytes = 0;
  size_t _totalBlocks = 0;
  uint64_t _version = V_CURRENT;

  // Public methods.
 public:
  // Some instantiations of `MapType` (the dense ones using stxxl or mmap)
  // require additional calls to setup() before being fully initialized.
  IndexMetaData() = default;

  // Pass all arguments that are needed for initialization to the underlying
  // implementation of `_data` (which is of type `MapType`).
  template <typename... dataArgs>
  void setup(dataArgs&&... args) {
    _data.setup(std::forward<dataArgs>(args)...);
  }

  // `isPersistentMetaData` is true when we do not need to add relation meta
  // data to _data, but assume that it is already contained in _data. This must
  // be a compile time parameter because we have to avoid instantation of member
  // function set() when `MapType` is read only  (e.g., when based on
  // MmapVectorView).
  template <bool isPersistentMetaData = false>
  void add(AddType addedValue);

  off_t getOffsetAfter() const;

  GetType getMetaData(Id col0Id) const;

  // Persistent meta data MapTypes (called MmapBased here) have to be separated
  // from RAM-based (e.g. hashMap based sparse) ones at compile time, this is
  // done in the following block.
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
  // This is used to check whether this is a really old index that requires
  // rebuilding.
  static constexpr uint64_t MAGIC_NUMBER_FOR_SERIALIZATION =
      _isMmapBased ? MAGIC_NUMBER_MMAP_META_DATA_VERSION
                   : MAGIC_NUMBER_SPARSE_META_DATA_VERSION;

  // Write meta data to file with given name (contents will be overwritten if
  // file exists).
  void writeToFile(const std::string& filename) const;

  // Write to the end of an already existing file. This will move the file
  // pointer to the end of that file.
  void appendToFile(ad_utility::File* file) const;

  // Read from file with the given name.
  void readFromFile(const std::string& filename);

  // Read from file, assuming that it is already open and has valid meta data at
  // the end. The call will change the position in the file.
  void readFromFile(ad_utility::File* file);

  bool col0IdExists(Id col0Id) const;

  // Calculate and save statistics that are expensive to calculate so we only
  // have to do this once during the index build and not at server start.
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

  // Symmetric serialization function for the ad_utility::serialization module.
  AD_SERIALIZE_FRIEND_FUNCTION(IndexMetaData) {
    // The binary format of an IndexMetaData start with an 8-byte magicNumber.
    // After this magic number, an 8-byte version number follows. Both have to
    // match.

    using T = IndexMetaData<M>;
    uint64_t magicNumber = T::MAGIC_NUMBER_FOR_SERIALIZATION;

    serializer | magicNumber;

    // This check might only become false, if we are reading from the serializer
    if (magicNumber != T::MAGIC_NUMBER_FOR_SERIALIZATION) {
      throw WrongFormatException(
          "The binary format of this index is no longer supported by QLever. "
          "Please rebuild the index.");
    }

    serializer | arg._version;
    // This check might only become false, if we are reading from the serializer
    if (arg.getVersion() != V_CURRENT) {
      throw WrongFormatException(
          "The binary format of this index is no longer supported by QLever. "
          "Please rebuild the index.");
    }

    // Serialize the rest of the data members
    serializer | arg._name;
    serializer | arg._data;
    serializer | arg._blockData;

    serializer | arg._offsetAfter;
    serializer | arg._totalElements;
    serializer | arg._totalBytes;
    serializer | arg._totalBlocks;
  }
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
