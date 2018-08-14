// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)
//

#include "./MetaDataConverter.h"
#include <string>
#include "./IndexMetaData.h"

using std::string;

// Conversion function for old binary indices
MmapHandler convertHmapHandlerToMmap(const MetaDataWrapperHashMap& hmap,
                                     const std::string& filename) {
  // first we need the maximal Id of the hashMap
  size_t maxId = 0;
  for (auto it = hmap.cbegin(); it != hmap.cend(); ++it) {
    if (it->first > maxId) {
      maxId = it->first;
    }
  }
  MmapHandler res;
  res.setup(maxId + 1, FullRelationMetaData::empty, filename);
  for (auto it = hmap.cbegin(); it != hmap.cend(); ++it) {
    res.set(it->first, it->second);
  }
  return res;
}

// _______________________________________________________________________
IndexMetaDataMmap convertHmapMetaDataToMmap(const IndexMetaDataHmap& hmap,
                                            const std::string& filename,
                                            bool verify) {
  IndexMetaDataMmap res;
  res._offsetAfter = hmap._offsetAfter;
  res._nofTriples = hmap._nofTriples;
  res._name = hmap._name;
  res._filename = hmap._filename;
  res._data = convertHmapHandlerToMmap(hmap._data, filename);
  res._blockData = hmap._blockData;

  if (verify) {
    for (auto it = res._data.cbegin(); it != res._data.cend(); ++it) {
      if (hmap._data.getAsserted(it->first) != it->second) {
        std::cerr << "mismatch in converted Meta data, exiting\n";
        exit(1);
      }
    }
    for (auto it = hmap._data.cbegin(); it != hmap._data.cend(); ++it) {
      if (res._data.getAsserted(it->first) != it->second) {
        std::cerr << "mismatch in converted Meta data, exiting\n";
        exit(1);
      }
    }
  }
  return res;
}

// ______________________________________________________________________
void convertHmapBasedPermutatationToMmap(const string& permutIn,
                                         const string& permutOut,
                                         const string& mmap, bool verify) {
  IndexMetaDataHmap h;
  h.readFromFile(permutIn);
  IndexMetaDataMmap m = convertHmapMetaDataToMmap(h, mmap, verify);
  writeNewPermutation(permutIn, permutOut, m);
}

// _________________________________________________________________________
void addMagicNumberToSparseMetaDataPermutation(const string& permutIn,
                                               const string& permutOut) {
  IndexMetaDataHmap h;
  h.readFromFile(permutIn);
  writeNewPermutation(permutIn, permutOut, h);
}

// ________________________________________________________________________
template <class MetaData>
void writeNewPermutation(const string& oldPermutation,
                         const string& newPermutation,
                         const MetaData& metaData) {
  ad_utility::File oldFile(oldPermutation, "r");
  ad_utility::File newFile(newPermutation, "w");

  // 1 GB of Buffer
  static const size_t BufferSize = 1 << 30;

  // get the position where the meta data starts
  // everything BEFORE that position is the data of the relation and has to be
  // copied
  off_t metaFrom;
  oldFile.getLastOffset(&metaFrom);
  AD_CHECK(oldFile.seek(0, SEEK_SET));
  AD_CHECK(newFile.seek(0, SEEK_SET));

  // always copy in chunks of BufferSize
  auto remainingBytes = static_cast<size_t>(metaFrom);
  unsigned char* buf = new unsigned char[BufferSize];
  while (remainingBytes >= BufferSize) {
    oldFile.read(buf, BufferSize);
    newFile.write(buf, BufferSize);
    remainingBytes -= BufferSize;
  }
  oldFile.read(buf, remainingBytes);
  newFile.write(buf, remainingBytes);
  delete[] buf;
  metaData.appendToFile(&newFile);
}
