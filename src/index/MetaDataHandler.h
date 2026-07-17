//  Copyright 2018 - 2026 The QLever Authors, in particular:
//
//  2018 - 2023 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//  2018 - 2023 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//  2026        Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_METADATAHANDLER_H
#define QLEVER_SRC_INDEX_METADATAHANDLER_H

#include <optional>
#include <vector>

#include "index/CompressedRelation.h"
#include "util/File.h"

// Container for the `CompressedRelationMetadata` objects (one per relation) of
// a single permutation. The objects are kept in a `std::vector` that is sorted
// by `col0Id_`, which allows for binary search by `col0Id_`. The metadata of a
// permutation is small enough to be held completely in RAM. It is stored in a
// separate file next to the permutation (see `META_FILE_SUFFIX`) via
// `readFromFile` and `writeToFile`. For backwards compatibility, that file uses
// the exact same on-disk layout that the (now removed) `ad_utility::MmapVector`
// used: the array of metadata objects, followed by an
// `ad_utility::MmapVectorMetaData` trailer at the very end of the file.
class MetaDataWrapperDense {
 private:
  // The metadata objects, sorted by `col0Id_`.
  std::vector<CompressedRelationMetadata> vec_;

 public:
  MetaDataWrapperDense() = default;
  MetaDataWrapperDense(MetaDataWrapperDense&&) = default;
  MetaDataWrapperDense& operator=(MetaDataWrapperDense&&) = default;

  // Read the metadata from `file`. The number of elements is taken from the
  // `MmapVectorMetaData` trailer at the end of the file, the elements
  // themselves are stored at the beginning of the file.
  void readFromFile(ad_utility::File& file);

  // Write the metadata to `file`.
  void writeToFile(ad_utility::File& file) const;

  // ___________________________________________________________
  [[nodiscard]] size_t size() const { return vec_.size(); }

  // Add a metadata object. The objects have to be added in strictly ascending
  // order of their `col0Id_`, so that `vec_` stays sorted.
  void add(const CompressedRelationMetadata& value);

  // Return the metadata for the given `col0Id`, or `std::nullopt` if there is
  // no metadata for it.
  [[nodiscard]] std::optional<CompressedRelationMetadata> getIfPresent(
      Id col0Id) const;

  // Exchange the multiplicities of the last column with those of `other` (see
  // `IndexMetaData::exchangeMultiplicities`). Both wrappers must contain the
  // same `col0Id`s in the same order.
  void exchangeMultiplicities(MetaDataWrapperDense& other);

 private:
  [[nodiscard]] auto lower_bound(Id col0Id) const;
};

#endif  // QLEVER_SRC_INDEX_METADATAHANDLER_H
