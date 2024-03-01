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
  WrongFormatException(std::string msg) : msg_(std::move(msg)) {}
  const char* what() const throw() { return msg_.c_str(); }

 private:
  std::string msg_;
};

// Struct so that we can return this in a single variable.
struct VersionInfo {
  uint64_t version_;
  size_t nOfBytes_;
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
  using AddType = CompressedRelationMetadata;
  using GetType = const CompressedRelationMetadata&;
  using BlocksType = std::vector<CompressedBlockMetadata>;

  // Private member variables.
 private:
  off_t offsetAfter_ = 0;

  string name_;
  string filename_;

  // TODO: For each of the following two (data_ and blockData_), both the type
  // name and the variable name are terrible.

  // For each relation, its meta data.
  MapType data_;
  // For each compressed block, its meta data.
  BlocksType blockData_;

  size_t totalElements_ = 0;
  size_t numDistinctCol0_ = 0;
  uint64_t version_ = V_CURRENT;

  // Public methods.
 public:
  // Some instantiations of `MapType` (the dense ones using stxxl or mmap)
  // require additional calls to setup() before being fully initialized.
  IndexMetaData() = default;

  // Pass all arguments that are needed for initialization to the underlying
  // implementation of `data_` (which is of type `MapType`).
  template <typename... dataArgs>
  void setup(dataArgs&&... args) {
    data_.setup(std::forward<dataArgs>(args)...);
  }

  // `isPersistentMetaData` is true when we do not need to add relation meta
  // data to data_, but assume that it is already contained in data_. This must
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
      MetaDataWrapperDense<ad_utility::MmapVector<CompressedRelationMetadata>>;
  using MetaWrapperMmapView = MetaDataWrapperDense<
      ad_utility::MmapVectorView<CompressedRelationMetadata>>;
  template <typename T>
  struct IsMmapBased {
    static const bool value = std::is_same<MetaWrapperMmap, T>::value ||
                              std::is_same<MetaWrapperMmapView, T>::value;
  };
  // Compile time information whether this instatiation if MMapBased or not
  static constexpr bool isMmapBased_ = IsMmapBased<MapType>::value;

  // This magic number is written when serializing the IndexMetaData to a file.
  // This is used to check whether this is a really old index that requires
  // rebuilding.
  static constexpr uint64_t MAGIC_NUMBER_FOR_SERIALIZATION =
      isMmapBased_ ? MAGIC_NUMBER_MMAP_META_DATA_VERSION
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
  void calculateStatistics(size_t numDistinctCol0);

  // The number of distinct Col0Ids has to be passed in manually, as it cannot
  // be computed.
  string statistics() const;

  void setName(const string& name) { name_ = name; }

  const string& getName() const { return name_; }

  size_t getVersion() const { return version_; }

  const MapType& data() const { return data_; }

  BlocksType& blockData() { return blockData_; }
  const BlocksType& blockData() const { return blockData_; }

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

    serializer | arg.version_;
    // This check might only become false, if we are reading from the serializer
    if (arg.getVersion() != V_CURRENT) {
      throw WrongFormatException(
          "The binary format of this index is no longer supported by QLever. "
          "Please rebuild the index.");
    }

    // Serialize the rest of the data members
    serializer | arg.name_;
    serializer | arg.data_;
    serializer | arg.blockData_;
    serializer | arg.offsetAfter_;
    serializer | arg.totalElements_;
    serializer | arg.numDistinctCol0_;
  }
};

// ___________________________________________________________________________
template <class MapType>
ad_utility::File& operator<<(ad_utility::File& f,
                             const IndexMetaData<MapType>& imd);

// aliases for easier use in Index class
using MetaWrapperMmap =
    MetaDataWrapperDense<ad_utility::MmapVector<CompressedRelationMetadata>>;
using MetaWrapperMmapView = MetaDataWrapperDense<
    ad_utility::MmapVectorView<CompressedRelationMetadata>>;
using IndexMetaDataMmap = IndexMetaData<MetaWrapperMmap>;
using IndexMetaDataMmapView = IndexMetaData<MetaWrapperMmapView>;

#include "./IndexMetaDataImpl.h"
