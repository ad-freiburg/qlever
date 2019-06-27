// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#pragma once

#include "../util/File.h"
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

// specialization for MMapBased Arrays which only read
// block-based data from Memory
template <class MapType>
void IndexMetaData<MapType>::createFromByteBuffer(unsigned char* buf) {
  // read magic number
  auto v = parseMagicNumberAndVersioning(buf);
  auto version = v._version;
  _version = version;
  buf += v._nOfBytes;

  size_t nameLength = readFromBuf<size_t>(&buf);
  _name.assign(reinterpret_cast<char*>(buf), nameLength);
  buf += nameLength;

  size_t nofRelations = readFromBuf<size_t>(&buf);
  if constexpr (_isMmapBased) {
    _data.setSize(nofRelations);
  }
  _offsetAfter = readFromBuf<off_t>(&buf);

  if constexpr (!_isMmapBased) {
    // HashMap-based means that FullRMD and Blocks are all stored withing the
    // permutation file
    for (size_t i = 0; i < nofRelations; ++i) {
      FullRelationMetaData rmd;
      rmd.createFromByteBuffer(buf);
      buf += rmd.bytesRequired();
      if (rmd.hasBlocks()) {
        BlockBasedRelationMetaData bRmd;
        bRmd.createFromByteBuffer(buf);
        buf += bRmd.bytesRequired();
        add(rmd, bRmd);
      } else {
        add(rmd, BlockBasedRelationMetaData());
      }
    }
  } else {
    // MmapBased
    if (version < V_BLOCK_LIST_AND_STATISTICS) {
      for (auto it = _data.cbegin(); it != _data.cend(); ++it) {
        const FullRelationMetaData& rmd = (*it).second;
        if (rmd.hasBlocks()) {
          BlockBasedRelationMetaData bRmd;
          bRmd.createFromByteBuffer(buf);
          buf += bRmd.bytesRequired();
          // we do not need to add the meta data since it is already in _data
          // because of the persisten MMap file
          add<true>(rmd, bRmd);
        } else {
          add<true>(rmd, BlockBasedRelationMetaData());
        }
      }
      calculateExpensiveStatistics();
    } else {
      // version >= V_BLOCK_LIST_AND_STATISTICS, no need to touch Relations that
      // don't have blocks
      size_t numBlockData = readFromBuf<size_t>(&buf);
      for (size_t i = 0; i < numBlockData; ++i) {
        Id id = readFromBuf<Id>(&buf);
        BlockBasedRelationMetaData bRmd;
        bRmd.createFromByteBuffer(buf);
        buf += bRmd.bytesRequired();
        // we do not need to add the meta data since it is already in _data
        // because of the persisten MMap file
        add<true>(_data.getAsserted(id), bRmd);
      }
    }
  }
  if (version >= V_BLOCK_LIST_AND_STATISTICS) {
    _totalElements = readFromBuf<size_t>(&buf);
    _totalBytes = readFromBuf<size_t>(&buf);
    _totalBlocks = readFromBuf<size_t>(&buf);
  } else {
    calculateExpensiveStatistics();
  }
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

// _____________________________________________________________________________
template <class MapType>
ad_utility::File& operator<<(ad_utility::File& f,
                             const IndexMetaData<MapType>& imd) {
  // first write magic number
  if constexpr (IndexMetaData<MapType>::_isMmapBased) {
    f.write(&MAGIC_NUMBER_MMAP_META_DATA_VERSION,
            sizeof(MAGIC_NUMBER_MMAP_META_DATA_VERSION));
  } else {
    f.write(&MAGIC_NUMBER_SPARSE_META_DATA_VERSION,
            sizeof(MAGIC_NUMBER_SPARSE_META_DATA_VERSION));
  }
  // write version
  f.write(&V_CURRENT, sizeof(V_CURRENT));
  size_t nameLength = imd._name.size();
  f.write(&nameLength, sizeof(nameLength));
  f.write(imd._name.data(), nameLength);
  size_t nofElements = imd._data.size();
  f.write(&nofElements, sizeof(nofElements));
  f.write(&imd._offsetAfter, sizeof(imd._offsetAfter));
  if constexpr (!IndexMetaData<MapType>::_isMmapBased) {
    for (auto it = imd._data.cbegin(); it != imd._data.cend(); ++it) {
      const auto el = *it;
      f << el.second;

      if (el.second.hasBlocks()) {
        auto itt = imd._blockData.find(el.second._relId);
        AD_CHECK(itt != imd._blockData.end());
        f << itt->second;
      }
    }
  } else {
    size_t numBlockData = imd._blockData.size();
    f.write(&numBlockData, sizeof(numBlockData));
    for (const auto& [id, blockData] : imd._blockData) {
      f.write(&id, sizeof(id));
      f << blockData;
    }
  }
  f.write(&imd._totalElements, sizeof(imd._totalElements));
  f.write(&imd._totalBytes, sizeof(&imd._totalBytes));
  f.write(&imd._totalBlocks, sizeof(&imd._totalBlocks));

  return f;
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
  (*file) << *this;
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
  unsigned char* buf = new unsigned char[metaTo - metaFrom];
  file->read(buf, static_cast<size_t>(metaTo - metaFrom), metaFrom);
  createFromByteBuffer(buf);
  delete[] buf;
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

// ___________________________________________________________________
template <class MapType>
VersionInfo IndexMetaData<MapType>::parseMagicNumberAndVersioning(
    unsigned char* buf) {
  uint64_t magicNumber = *reinterpret_cast<uint64_t*>(buf);
  size_t nOfBytes = 0;
  bool hasVersion = false;
  if constexpr (!_isMmapBased) {
    if (magicNumber == MAGIC_NUMBER_MMAP_META_DATA ||
        magicNumber == MAGIC_NUMBER_MMAP_META_DATA_VERSION) {
      throw WrongFormatException(
          "ERROR: magic number of MetaData indicates that we are trying "
          "to construct a hashMap based IndexMetaData from  mmap-based meta "
          "data. This is not valid."
          "Please use ./MetaDataConverterMain"
          "to convert old indices without rebuilding them (See README.md).\n");
      AD_CHECK(false);
    } else if (magicNumber == MAGIC_NUMBER_SPARSE_META_DATA) {
      hasVersion = false;
      nOfBytes = sizeof(uint64_t);
    } else if (magicNumber == MAGIC_NUMBER_SPARSE_META_DATA_VERSION) {
      hasVersion = true;
      nOfBytes = sizeof(uint64_t);
    } else {
      // no magic number found
      hasVersion = false;
      nOfBytes = 0;
    }
  } else {  // this _isMmapBased
    if (magicNumber == MAGIC_NUMBER_MMAP_META_DATA) {
      hasVersion = false;
      nOfBytes = sizeof(uint64_t);
    } else if (magicNumber == MAGIC_NUMBER_MMAP_META_DATA_VERSION) {
      hasVersion = true;
      nOfBytes = sizeof(uint64_t);
    } else {
      throw WrongFormatException(
          "ERROR: No or wrong magic number found in persistent "
          "mmap-based meta data. "
          "Please use ./MetaDataConverterMain "
          "to convert old indices without rebuilding them (See "
          "README.md).Terminating...\n");
    }
  }

  VersionInfo res;
  res._nOfBytes = nOfBytes;
  if (!hasVersion) {
    res._version = V_NO_VERSION;
  } else {
    res._version = *reinterpret_cast<uint64_t*>(buf + res._nOfBytes);
    res._nOfBytes += sizeof(uint64_t);
  }
  if (res._version < V_CURRENT) {
    LOG(INFO)
        << "WARNING: your IndexMetaData seems to have an old format (version "
           "tag < V_CURRENT). Please consider using ./MetaDataConverterMain to "
           "benefit from improvements in the index structure.\n";

  } else if (res._version > V_CURRENT) {
    LOG(INFO) << "ERROR: version tag does not match any actual version (> "
                 "V_CURRENT). Your IndexMetaData is probably corrupted. "
                 "Terminating\n";
    AD_CHECK(false);
  }
  return res;
}
