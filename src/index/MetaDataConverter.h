// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)
//

#include <string>
#include "./IndexMetaData.h"

using ad_utility::MmapVector;
using MmapHandler = MetaDataWrapperDense<MmapVector<FullRelationMetaData>>;
// _______________________________________________________________________
MmapHandler convertHmapHandlerToMmap(const MetaWrapperHashMap& hmap,
                                     const std::string& filename);

// _______________________________________________________________________
MmapHandler convertHmapHandlerToMmap(const MmapHandler& mmap);

// _______________________________________________________________________
IndexMetaDataMmap convertHmapMetaDataToMmap(const IndexMetaDataHmap& hmap,
                                            const std::string& filename,
                                            bool verify);

// _______________________________________________________________________
IndexMetaDataHmap convertMmapMetaDataToHmap(const IndexMetaDataMmap& mmap,
                                            bool verify);

// Convert hashmap based permutation to mmap-based permutation
// Arguments:
//   permutIn: path to permutation with hash map based meta data
//   permutOut: path to File where mmap based permutation is written. This file
//            will be overwritten
//   mmap : path to file were the persistent mmap vector will be stored (will be
//     overwritten)
void convertPermutationToMmap(const string& permutIn, const string& permutOut,
                              const string& mmap, bool verify = true);

void convertPermutationToHmap(const string& permutIn, const string& permutOut,
                              bool verify = true);

// Copy hashMap based permutation and update meta data format (add magic number)
// permutIn is read, permutOut is (over)written.
void addMagicNumberToHmapMetaDataPermutation(const string& permutIn,
                                             const string& permutOut);
// Copy the permutation data from oldPermutation to newPermutation and add the
// meta data
template <class MetaData>
void writeNewPermutation(const string& oldPermutation,
                         const string& newPermutation, const MetaData& meta);

// __________________________________________________________________________
inline void notifyCreated(const string& filename, bool hasConvertedSuffix) {
  if (hasConvertedSuffix) {
    std::cout << "created new file " << filename
              << ".converted . This has to be manually renamed to " << filename
              << " in order to use the updated index. Please consider backing "
                 "up the original file "
              << filename << "\n\n";
  } else {
    std::cout << "created new file " << filename
              << " . This file already has its final name and does not need to "
                 "be renamed.\n\n";
  }
}

// ___________________________________________________________________________
inline void notifyUnneccessary(const string& filename) {
  std::cout
      << "File " << filename
      << " is not needed anymore with the new index format. It can safely be "
         "removed after making sure that  the converted index works properly\n";
}

// checks if a configuration file is present. If not, the vocabulary is
// compressed and the necessary configuration is written to a new configuration
// file
void CompressVocabAndCreateConfigurationFile(const string& indexPrefix);
