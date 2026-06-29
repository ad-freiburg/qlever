//  Copyright 2015 - 2026 The QLever Authors, in particular:
//
//  2015 Björn Buchhold <buchhold@cs.uni-freiburg.de>, UFR
//  2018 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_INDEXMETADATA_H
#define QLEVER_SRC_INDEX_INDEXMETADATA_H

#include <cmath>
#include <exception>
#include <limits>
#include <optional>
#include <utility>

#include "global/Id.h"
#include "index/CompressedRelation.h"
#include "index/MetaDataHandler.h"
#include "util/File.h"
#include "util/Serializer/Serializer.h"

// An exception is thrown when an index in an old, no longer supported binary
// format is read.
class WrongFormatException : public std::exception {
 public:
  explicit WrongFormatException(std::string msg) : msg_{std::move(msg)} {}
  const char* what() const noexcept override { return msg_.c_str(); }

 private:
  std::string msg_;
};

// Struct so that we can return this in a single variable.
struct VersionInfo {
  uint64_t version_;
  size_t nOfBytes_;
};

// Magic number to separate different types of metadata.
constexpr uint64_t MAGIC_NUMBER_FOR_SERIALIZATION =
    std::numeric_limits<uint64_t>::max() - 2;

// Constant for the current version.
constexpr uint64_t V_CURRENT = 2;

// The metadata for an index permutation. The per-relation metadata is held in
// RAM by `MetaDataWrapperDense` (stored in a separate file next to the
// permutation), the per-block metadata is part of this object's serialization
// (stored at the end of the permutation file).
class IndexMetaData {
 public:
  using BlocksType = std::vector<CompressedBlockMetadata>;

 private:
  std::string name_;

  // TODO: For each of the following two (data_ and blockData_), both the type
  // name and the variable name are terrible.

  // For each relation, its metadata.
  MetaDataWrapperDense data_;
  // For each compressed block, its metadata.
  std::shared_ptr<BlocksType> blockData_ = std::make_shared<BlocksType>();

  size_t totalElements_ = 0;
  size_t numDistinctCol0_ = 0;
  uint64_t version_ = V_CURRENT;

 public:
  IndexMetaData() = default;

  void add(const CompressedRelationMetadata& addedValue);

  // Return the metadata for this id if it has been stored.
  std::optional<CompressedRelationMetadata> getMetaDataIfPresent(
      Id col0Id) const;

  // Write the metadata to the file `filename` (for the per-block metadata,
  // which is appended to the end of the file) and to `filename +
  // META_FILE_SUFFIX` (for the per-relation metadata). Existing contents are
  // overwritten.
  void writeToFile(const std::string& filename) const;

  // Append the per-block metadata to the end of `permutationFile` (moving its
  // file pointer to the end) and write the per-relation metadata to
  // `metaFile` (overwriting it).
  void appendToFile(ad_utility::File& permutationFile,
                    ad_utility::File& metaFile) const;

  // Read the metadata written by `writeToFile` from `filename` and `filename +
  // META_FILE_SUFFIX`.
  void readFromFile(const std::string& filename);

  // Read the metadata, assuming that `permutationFile` is already open and has
  // valid per-block metadata at its end, and that `metaFile` holds the
  // per-relation metadata. The call will change the position in
  // `permutationFile`. This ensures the format is backwards compatible.
  void readFromFile(ad_utility::File& permutationFile,
                    ad_utility::File& metaFile);

  // Calculate and save statistics that are expensive to calculate so we only
  // have to do this once during the index build and not at server start.
  void calculateStatistics(size_t numDistinctCol0);

  // The number of distinct Col0Ids has to be passed in manually, as it cannot
  // be computed.
  std::string statistics() const;

  void setName(const std::string& name) { name_ = name; }

  const std::string& getName() const { return name_; }

  BlocksType& blockData() { return *blockData_; }
  const BlocksType& blockData() const { return *blockData_; }
  std::shared_ptr<const BlocksType> blockDataShared() const {
    return blockData_;
  }

  size_t totalElements() const { return totalElements_; }

  // Exchange the multiplicities for two permutations that are "twins" (e.g. PSO
  // and POS). This is needed because the multiplicity of the last column is
  // stored in the metadata of the other permutation. The `PairMetadataWriter`
  // already does this for the `PermutationWriter<true>` but when both
  // permutations are written individually, we need to exchange the
  // multiplicities of col 1 and col2 in post-processing.
  void exchangeMultiplicities(IndexMetaData& other);

  // Symmetric serialization function for the ad_utility::serialization module.
  AD_SERIALIZE_FRIEND_FUNCTION(IndexMetaData) {
    // The binary format of an IndexMetaData start with an 8-byte magicNumber.
    // After this magic number, an 8-byte version number follows. Both have to
    // match.

    uint64_t magicNumber = MAGIC_NUMBER_FOR_SERIALIZATION;

    serializer | magicNumber;

    // This check might only become false, if we are reading from the serializer
    if (magicNumber != MAGIC_NUMBER_FOR_SERIALIZATION) {
      throw WrongFormatException(
          "The binary format of this index is no longer supported by QLever. "
          "Please rebuild the index.");
    }

    serializer | arg.version_;
    // This check might only become false, if we are reading from the serializer
    if (arg.version_ != V_CURRENT) {
      throw WrongFormatException(
          "The binary format of this index is no longer supported by QLever. "
          "Please rebuild the index.");
    }

    // Serialize the rest of the data members. The per-relation metadata
    // (`data_`) is not part of this serialization; it lives in its own file and
    // is handled separately by `appendToFile` / `readFromFile`.
    serializer | arg.name_;
    serializer | arg.blockData();
    // This class no longer needs this member but to keep the format compatible
    // we need to consume/write this value here.
    [[maybe_unused]] off_t offsetAfter = 0;
    serializer | offsetAfter;
    serializer | arg.totalElements_;
    serializer | arg.numDistinctCol0_;
  }
};

#endif  // QLEVER_SRC_INDEX_INDEXMETADATA_H
