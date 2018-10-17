// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)
//

#include "./MetaDataConverter.h"
#include <nlohmann/json.hpp>
#include <string>
#include "../global/Constants.h"
#include "./CompressedString.h"
#include "./IndexMetaData.h"
#include "./PrefixHeuristic.h"
#include "./Vocabulary.h"

using std::string;
using json = nlohmann::json;

// Conversion function for old binary indices
MmapHandler convertHmapHandlerToMmap(const MetaDataWrapperHashMapSparse& hmap,
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
  notifyCreated(filename, false);

  for (auto it = hmap.cbegin(); it != hmap.cend(); ++it) {
    res.set(it->first, it->second);
  }

  return res;
}

// ___________________________________________________________________________
MetaDataWrapperHashMapSparse convertMmapHandlerToHmap(const MmapHandler& mmap) {
  MetaDataWrapperHashMapSparse res;
  for (auto it = mmap.cbegin(); it != mmap.cend(); ++it) {
    res.set(it->first, it->second);
  }
  return res;
}

// _______________________________________________________________________
IndexMetaDataMmap convertHmapMetaDataToMmap(const IndexMetaDataHmapSparse& hmap,
                                            const std::string& filename,
                                            bool verify) {
  IndexMetaDataMmap res;
  res._offsetAfter = hmap._offsetAfter;
  res._totalElements = hmap._totalElements;
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

// _______________________________________________________________________
IndexMetaDataHmapSparse convertMmapMetaDataToHmap(const IndexMetaDataMmap& mmap,
                                                  bool verify) {
  IndexMetaDataHmapSparse res;
  res._offsetAfter = mmap._offsetAfter;
  res._totalElements = mmap._totalElements;
  res._name = mmap._name;
  res._filename = mmap._filename;
  res._data = convertMmapHandlerToHmap(mmap._data);
  res._blockData = mmap._blockData;

  if (verify) {
    for (auto it = res._data.cbegin(); it != res._data.cend(); ++it) {
      if (mmap._data.getAsserted(it->first) != it->second) {
        std::cerr << "mismatch in converted Meta data, exiting\n";
        exit(1);
      }
    }
    for (auto it = mmap._data.cbegin(); it != mmap._data.cend(); ++it) {
      if (res._data.getAsserted(it->first) != it->second) {
        std::cerr << "mismatch in converted Meta data, exiting\n";
        exit(1);
      }
    }
  }
  notifyUnneccessary(mmap._data.getFilename());
  return res;
}

// ______________________________________________________________________
void convertPermutationToMmap(const string& permutIn, const string& permutOut,
                              const string& mmap, bool verify) {
  try {
    IndexMetaDataHmapSparse h;
    h.readFromFile(permutIn);
    IndexMetaDataMmap m = convertHmapMetaDataToMmap(h, mmap, verify);
    writeNewPermutation(permutIn, permutOut, m);
  } catch (const WrongFormatException& e) {
    std::cerr << "this is not a sparse permutation, Trying to read as Mmap";
    IndexMetaDataMmap m;
    m.readFromFile(permutIn);
    if (m.getVersion() < V_CURRENT) {
      writeNewPermutation(permutIn, permutOut, m);
    }
  }
}

// _________________________________________________________________________
void convertPermutationToHmap(const string& permutIn, const string& permutOut,
                              bool verify) {
  try {
    IndexMetaDataHmapSparse h;
    h.readFromFile(permutIn);
    writeNewPermutation(permutIn, permutOut, h);
  } catch (const WrongFormatException& e) {
    std::cerr << "this is not a sparse permutation, Trying to read as Mmap";
    IndexMetaDataMmap m;
    m.readFromFile(permutIn);
    IndexMetaDataHmapSparse h = convertMmapMetaDataToHmap(m, verify);
    writeNewPermutation(permutIn, permutOut, h);
  }
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
  notifyCreated(oldPermutation, true);
}

// ____________________________________________________________________
void CompressVocabAndCreateConfigurationFile(const string& indexPrefix) {
  string confFilename = indexPrefix + CONFIGURATION_FILE;
  string vocabFilename = indexPrefix + ".vocabulary";
  if (ad_utility::File::exists(confFilename)) {
    std::cout << "This index already has a configuration file, check if it\n"
                 "contains prefixes as internal list instead of in a separate\n"
                 ".prefixes file\n";

    std::ifstream confFile(indexPrefix + CONFIGURATION_FILE);
    AD_CHECK(confFile.is_open());
    json config;
    confFile >> config;
    if (config.find("prefixes") == config.end()) {
      std::cout << "The configuration file " << confFilename
                << " is missing the \"prefixes\" field" << std::endl;
      AD_CHECK(false);
    }
    auto prefixes = config["prefixes"];
    if (prefixes.type() == json::value_t::boolean &&
        ad_utility::File::exists(indexPrefix + PREFIX_FILE)) {
      std::cout << "The index already uses a separate " << PREFIX_FILE
                << " file\n";
    } else if (prefixes.type() == json::value_t::array) {
      std::cout << "Converting to separate " << PREFIX_FILE << " file\n";
      std::ofstream prefixFile(indexPrefix + PREFIX_FILE);
      AD_CHECK(prefixFile.is_open());
      for (const string& prefix : prefixes) {
        prefixFile << prefix << '\n';
      }
    } else {
      std::cout << "The configuration file " << confFilename
                << " has an unrecoverably broken \"prefixes\" field"
                << std::endl;
      AD_CHECK(false);
    }

  } else {
    std::cout << "This index does not have a configuration file. We have to "
                 "create it and also compress the vocabulary\n";

    json j;
    j["external-literals"] =
        ad_utility::File::exists(indexPrefix + ".literals-index");
    auto prefixes =
        calculatePrefixes(vocabFilename, NUM_COMPRESSION_PREFIXES, 1);
    j["prefixes"] = prefixes;
    Vocabulary<CompressedString>::prefixCompressFile(
        vocabFilename, vocabFilename + ".converted", prefixes);
    notifyCreated(vocabFilename, true);
    std::ofstream f(confFilename);
    AD_CHECK(f.is_open());
    f << j;
    notifyCreated(confFilename, false);
  }
}
