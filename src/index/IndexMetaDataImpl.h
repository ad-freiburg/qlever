// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#pragma once

#include "index/IndexMetaData.h"
#include "index/MetaDataHandler.h"
#include "util/File.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeHashMap.h"
#include "util/Serializer/SerializeString.h"

// _____________________________________________________________________________
template <class MapType>
template <bool isPersistentMetaData>
void IndexMetaData<MapType>::add(AddType addedValue) {
  // only add rmd to _data if it's not already present there
  if constexpr (!isPersistentMetaData) {
    totalElements_ += addedValue.getNofElements();
    data_.set(addedValue.col0Id_, addedValue);
  }
}

// _____________________________________________________________________________
template <class MapType>
off_t IndexMetaData<MapType>::getOffsetAfter() const {
  return offsetAfter_;
}

// _____________________________________________________________________________
template <class MapType>
typename IndexMetaData<MapType>::GetType IndexMetaData<MapType>::getMetaData(
    Id col0Id) const {
  return data_.getAsserted(col0Id);
}

// _____________________________________________________________________________
template <class MapType>
bool IndexMetaData<MapType>::col0IdExists(Id col0Id) const {
  return data_.count(col0Id) > 0;
}

// ____________________________________________________________________________
template <class MapType>
void IndexMetaData<MapType>::writeToFile(const std::string& filename) const {
  ad_utility::File file;
  file.open(filename.c_str(), "w");
  appendToFile(&file);
  file.close();
}

// ____________________________________________________________________________
template <class MapType>
void IndexMetaData<MapType>::appendToFile(ad_utility::File* file) const {
  AD_CONTRACT_CHECK(file->isOpen());
  file->seek(0, SEEK_END);
  off_t startOfMeta = file->tell();
  ad_utility::serialization::FileWriteSerializer serializer{std::move(*file)};
  serializer << (*this);
  *file = std::move(serializer).file();
  file->write(&startOfMeta, sizeof(startOfMeta));
}

// _________________________________________________________________________
template <class MapType>
void IndexMetaData<MapType>::readFromFile(const std::string& filename) {
  ad_utility::File file;
  file.open(filename.c_str(), "r");
  readFromFile(&file);
  file.close();
}

// _________________________________________________________________________
template <class MapType>
void IndexMetaData<MapType>::readFromFile(ad_utility::File* file) {
  off_t metaFrom;
  off_t metaTo = file->getLastOffset(&metaFrom);
  std::vector<char> buf(metaTo - metaFrom);
  file->read(buf.data(), static_cast<size_t>(metaTo - metaFrom), metaFrom);

  ad_utility::serialization::ByteBufferReadSerializer serializer{
      std::move(buf)};

  serializer >> (*this);
}

// _____________________________________________________________________________
template <class MapType>
string IndexMetaData<MapType>::statistics() const {
  std::ostringstream os;
  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  os.imbue(locWithNumberGrouping);
  os << "#relations = " << numDistinctCol0_
     << ", #blocks = " << blockData_.size()
     << ", #triples = " << totalElements_;
  return std::move(os).str();
}

// __________________________________________________________________
template <class MapType>
void IndexMetaData<MapType>::calculateStatistics(size_t numDistinctCol0) {
  totalElements_ = 0;
  numDistinctCol0_ = numDistinctCol0;
  for (const auto& block : blockData_) {
    totalElements_ += block.numRows_;
  }
}
