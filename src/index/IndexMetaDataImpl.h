// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#pragma once

#include "../util/File.h"
#include "../util/Serializer/FileSerializer.h"
#include "../util/Serializer/SerializeHashMap.h"
#include "../util/Serializer/SerializeString.h"
#include "./IndexMetaData.h"
#include "./MetaDataHandler.h"

// _____________________________________________________________________________
template <class MapType>
template <bool persistentRMD>
void IndexMetaData<MapType>::add(const FullRelationMetaData& rmd,
                                 const BlockBasedRelationMetaData& bRmd) {
  // only add rmd to _data if it's not already present there
  if constexpr (!persistentRMD) {
    _data.set(rmd._relId, rmd);
  }

  off_t afterExpected =
      rmd.hasBlocks() ? bRmd._offsetAfter
                      : static_cast<off_t>(rmd._startFullIndex +
                                           rmd.getNofBytesForFulltextIndex());
  if (rmd.hasBlocks()) {
    _blockData[rmd._relId] = bRmd;
  }
  if (afterExpected > _offsetAfter) {
    _offsetAfter = afterExpected;
  }
}

// _____________________________________________________________________________
template <class MapType>
off_t IndexMetaData<MapType>::getOffsetAfter() const {
  return _offsetAfter;
}

// _____________________________________________________________________________
template <class MapType>
const RelationMetaData IndexMetaData<MapType>::getRmd(Id relId) const {
  const FullRelationMetaData& full = _data.getAsserted(relId);
  RelationMetaData ret(full);
  if (full.hasBlocks()) {
    ret._rmdBlocks = &_blockData.find(relId)->second;
  }
  return ret;
}

// _____________________________________________________________________________
template <class MapType>
bool IndexMetaData<MapType>::relationExists(Id relId) const {
  return _data.count(relId) > 0;
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
  AD_CHECK(file->isOpen());
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
  os << '\n';
  os << "-------------------------------------------------------------------\n";
  os << "----------------------------------\n";
  os << "Index Statistics:\n";
  os << "----------------------------------\n\n";
  os << "# Relations (_data.size()): " << _data.size() << '\n';
  os << "# Block Data: " << _blockData.size() << '\n';

  size_t totalPairIndexBytes = _totalElements * 2 * sizeof(Id);

  os << "# Elements:  " << _totalElements << '\n';
  os << "# Blocks:    " << _totalBlocks << "\n\n";
  os << "Theoretical size of Id triples: " << _totalElements * 3 * sizeof(Id)
     << " bytes \n";
  os << "Size of pair index:             " << totalPairIndexBytes
     << " bytes \n";
  os << "Total Size:                     " << _totalBytes << " bytes \n";
  os << "-------------------------------------------------------------------\n";
  return os.str();
}

// _____________________________________________________________________________
template <class MapType>
size_t IndexMetaData<MapType>::getNofBlocksForRelation(const Id id) const {
  auto it = _blockData.find(id);
  if (it != _blockData.end()) {
    return it->second._blocks.size();
  } else {
    return 0;
  }
}

// _____________________________________________________________________________
template <class MapType>
size_t IndexMetaData<MapType>::getTotalBytesForRelation(
    const FullRelationMetaData& frmd) const {
  auto it = _blockData.find(frmd._relId);
  if (it != _blockData.end()) {
    return static_cast<size_t>(it->second._offsetAfter - frmd._startFullIndex);
  } else {
    return frmd.getNofBytesForFulltextIndex();
  }
}

// ______________________________________________
template <class MapType>
size_t IndexMetaData<MapType>::getNofDistinctC1() const {
  return _data.size();
}

// __________________________________________________________________
template <class MapType>
void IndexMetaData<MapType>::calculateExpensiveStatistics() {
  _totalElements = 0;
  _totalBytes = 0;
  _totalBlocks = 0;
  for (auto it = _data.cbegin(); it != _data.cend(); ++it) {
    auto el = *it;
    _totalElements += el.second.getNofElements();
    _totalBytes += getTotalBytesForRelation(el.second);
    _totalBlocks += getNofBlocksForRelation(el.first);
  }
}

// ___________________________________________________________________________
template <typename Serializer, typename MapType>
void serialize(Serializer& serializer, IndexMetaData<MapType>& metaData) {
  // The binary format of an IndexMetaData start with an 8-byte magicNumber.
  // After this magic number, an 8-byte version number follows. Both have to
  // match.
  using T = IndexMetaData<MapType>;
  uint64_t magicNumber = T::MAGIC_NUMBER_FOR_SERIALIZATION;

  serializer | magicNumber;

  // This check might only become false, if we are reading from the serializer
  if (magicNumber != T::MAGIC_NUMBER_FOR_SERIALIZATION) {
    throw WrongFormatException(
        "The binary format of this index is no longer supported by QLever. "
        "Please rebuild the index.");
  }

  serializer | metaData._version;
  // This check might only become false, if we are reading from the serializer
  if (metaData.getVersion() != V_CURRENT) {
    throw WrongFormatException(
        "The binary format of this index is no longer supported by QLever. "
        "Please rebuild the index.");
  }

  // Serialize the rest of the data members
  serializer | metaData._name;
  serializer | metaData._data;
  serializer | metaData._blockData;

  serializer | metaData._offsetAfter;
  serializer | metaData._totalElements;
  serializer | metaData._totalBytes;
  serializer | metaData._totalBlocks;
}
