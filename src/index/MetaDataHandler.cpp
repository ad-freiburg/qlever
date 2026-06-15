//  Copyright 2018 - 2026 The QLever Authors, in particular:
//
//  2018 - 2023 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//  2018 - 2023 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//  2026        Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/MetaDataHandler.h"

#include <functional>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/MmapVector.h"
#include "util/Serializer/FileSerializer.h"

// _____________________________________________________________________________
auto MetaDataWrapperDense::lower_bound(Id col0Id) const {
  return ql::ranges::lower_bound(vec_, col0Id, {},
                                 &CompressedRelationMetadata::col0Id_);
}

// _____________________________________________________________________________
void MetaDataWrapperDense::readFromFile(ad_utility::File& file) {
  uint64_t numElements =
      ad_utility::MmapVectorMetaData::readFromFile(file).size_;
  ad_utility::serialization::FileReadSerializer serializer{std::move(file)};
  serializer.setSerializationPosition(0);
  vec_.resize(numElements);
  for (auto& metaData : vec_) {
    serializer | metaData;
  }
  file = std::move(serializer).file();
}

// _____________________________________________________________________________
void MetaDataWrapperDense::writeToFile(ad_utility::File& file) const {
  ad_utility::serialization::FileWriteSerializer serializer{std::move(file)};
  for (const auto& metaData : vec_) {
    serializer | metaData;
  }
  file = std::move(serializer).file();
  // Append the `MmapVector`-style trailer so that the file stays readable by
  // older QLever binaries. Only `size_` is read back by `readFromFile`.
  ad_utility::MmapVectorMetaData{
      vec_.size(), vec_.size(),
      vec_.size() * sizeof(CompressedRelationMetadata)}
      .writeToFile(file);
}

// _____________________________________________________________________________
void MetaDataWrapperDense::add(CompressedRelationMetadata value) {
  AD_CONTRACT_CHECK(vec_.empty() || vec_.back().col0Id_ < value.col0Id_);
  vec_.push_back(std::move(value));
}

// _____________________________________________________________________________
std::optional<CompressedRelationMetadata> MetaDataWrapperDense::getIfPresent(
    Id col0Id) const {
  auto it = lower_bound(col0Id);
  if (it != vec_.end() && it->col0Id_ == col0Id) {
    return *it;
  }
  return std::nullopt;
}

// _____________________________________________________________________________
void MetaDataWrapperDense::exchangeMultiplicities(MetaDataWrapperDense& other) {
  AD_CONTRACT_CHECK(vec_.size() == other.vec_.size(),
                    "Both IndexMetaData objects must have the same length.");
  for (auto&& [md1, md2] : ::ranges::views::zip(vec_, other.vec_)) {
    AD_CONTRACT_CHECK(md1.col0Id_.getBits() == md2.col0Id_.getBits(),
                      "The ids must be in the same order and the same ids must "
                      "be present in both IndexMetaData objects.");
    md1.multiplicityCol2_ = md2.multiplicityCol1_;
    md2.multiplicityCol2_ = md1.multiplicityCol1_;
  }
}
