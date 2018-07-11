// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#pragma once

#include "./IndexMetaData.h"
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

// _____________________________________________________________________________
template <class MapType>
void IndexMetaData<MapType>::createFromByteBufferSparse(unsigned char* buf) {
  size_t nofBytesDone = 0;
  // read magic number
  size_t magicNumber = *reinterpret_cast<size_t*>(buf);
  if (magicNumber == MAGIC_NUMBER_MMAP_META_DATA) {
    LOG(INFO)
        << "ERROR: magic number of MetaData indicates that we are trying "
           "to construct from  mmap-based meta data. This is not valid "
           "for sparse meta data formats. Please use ./MetaDataConverterMain "
           "to convert old indices without rebuilding them (See README.md). "
           "Terminating...\n";
    AD_CHECK(false);
  } else if (magicNumber == MAGIC_NUMBER_SPARSE_META_DATA) {
    // read valid magic number, skip it
    nofBytesDone += sizeof(magicNumber);
  } else {
    LOG(INFO)
        << "WARNING: No valid magic number found for meta data, this is "
           "probably "
           "an older index build. We should still be able to construct "
           "the meta data though. Please consider using "
           "./MetaDataConverterMain "
           "to convert old indices without rebuilding them (See README.md).\n";
  }
  size_t nameLength = *reinterpret_cast<size_t*>(buf + nofBytesDone);
  nofBytesDone += sizeof(size_t);
  _name.assign(reinterpret_cast<char*>(buf + nofBytesDone), nameLength);
  nofBytesDone += nameLength;
  size_t nofRelations = *reinterpret_cast<size_t*>(buf + nofBytesDone);
  nofBytesDone += sizeof(size_t);
  _offsetAfter = *reinterpret_cast<off_t*>(buf + nofBytesDone);
  nofBytesDone += sizeof(off_t);
  _nofTriples = 0;
  for (size_t i = 0; i < nofRelations; ++i) {
    FullRelationMetaData rmd;
    rmd.createFromByteBuffer(buf + nofBytesDone);
    _nofTriples += rmd.getNofElements();
    nofBytesDone += rmd.bytesRequired();
    if (rmd.hasBlocks()) {
      BlockBasedRelationMetaData bRmd;
      bRmd.createFromByteBuffer(buf + nofBytesDone);
      nofBytesDone += bRmd.bytesRequired();
      add(rmd, bRmd);
    } else {
      add(rmd, BlockBasedRelationMetaData());
    }
    }
}

// _____________________________________________________________________________
// specialization for MMapBased Arrays which only read
// block-based data from Memory
template <class MapType>
void IndexMetaData<MapType>::createFromByteBufferMmap(unsigned char* buf) {
  // read magic number
  size_t magicNumber = *reinterpret_cast<size_t*>(buf);
  if (magicNumber != MAGIC_NUMBER_MMAP_META_DATA) {
    LOG(INFO) << "ERROR: No or wrong magic number found in persistent "
                 "mmap-based meta data. "
                 "Please use ./MetaDataConverterMain "
                 "to convert old indices without rebuilding them (See "
                 "README.md).Terminating...\n";
    AD_CHECK(false);
  }
  size_t nofBytesDone = sizeof(MAGIC_NUMBER_MMAP_META_DATA);
  size_t nameLength = *reinterpret_cast<size_t*>(buf + nofBytesDone);
  nofBytesDone += sizeof(size_t);
  _name.assign(reinterpret_cast<char*>(buf + nofBytesDone), nameLength);
  nofBytesDone += nameLength;
  // skip nOfRelations, since this information is already stored in
  // _data  (_data is persistently set during index creation, because this is
  // the Mmap based specialization
  nofBytesDone += sizeof(size_t);
  _offsetAfter = *reinterpret_cast<off_t*>(buf + nofBytesDone);
  nofBytesDone += sizeof(off_t);
  _nofTriples = 0;

  // look for blockData in the already existing mmaped vector
  for (auto it = _data.cbegin(); it != _data.cend(); ++it) {
    const FullRelationMetaData& rmd = (*it).second;
    _nofTriples += rmd.getNofElements();
    if (rmd.hasBlocks()) {
      BlockBasedRelationMetaData bRmd;
      bRmd.createFromByteBuffer(buf + nofBytesDone);
      nofBytesDone += bRmd.bytesRequired();
      // we do not need to add the meta data since it is already in _data
      // because of the persisten MMap file
      add<true>(rmd, bRmd);
    } else {
      add<true>(rmd, BlockBasedRelationMetaData());
    }
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
  if constexpr (imd._isMmapBased) {
    f.write(&MAGIC_NUMBER_MMAP_META_DATA, sizeof(MAGIC_NUMBER_MMAP_META_DATA));
  } else {
    f.write(&MAGIC_NUMBER_SPARSE_META_DATA,
            sizeof(MAGIC_NUMBER_SPARSE_META_DATA));
  }
  size_t nameLength = imd._name.size();
  f.write(&nameLength, sizeof(nameLength));
  f.write(imd._name.data(), nameLength);
  size_t nofElements = imd._data.size();
  f.write(&nofElements, sizeof(nofElements));
  f.write(&imd._offsetAfter, sizeof(imd._offsetAfter));
  for (auto it = imd._data.cbegin(); it != imd._data.cend(); ++it) {
    const auto el = *it;
    // when we are MmapBased, the _data member is already persistent on disk, so
    // we do not write it here
    if constexpr (!imd._isMmapBased) {
      f << el.second;
    }

    if (el.second.hasBlocks()) {
      auto itt = imd._blockData.find(el.second._relId);
      AD_CHECK(itt != imd._blockData.end());
      f << itt->second;
    }
  }
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
  size_t totalElements = 0;
  size_t totalBytes = 0;
  size_t totalBlocks = 0;
  for (auto it = _data.cbegin(); it != _data.cend(); ++it) {
    auto el = *it;
    totalElements += el.second.getNofElements();
    totalBytes += getTotalBytesForRelation(el.second);
    totalBlocks += getNofBlocksForRelation(el.first);
  }
  size_t totalPairIndexBytes = totalElements * 2 * sizeof(Id);
  os << "# Elements:  " << totalElements << '\n';
  os << "# Blocks:    " << totalBlocks << "\n\n";
  os << "Theoretical size of Id triples: " << totalElements * 3 * sizeof(Id)
     << " bytes \n";
  os << "Size of pair index:             " << totalPairIndexBytes
     << " bytes \n";
  os << "Total Size:                     " << totalBytes << " bytes \n";
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
