// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)
//

#include <string>
#include "./IndexMetaData.h"

using ad_utility::MmapVector;
using MmapHandler = MetaDataWrapperDense<MmapVector<FullRelationMetaData>>;
// _______________________________________________________________________
MmapHandler convertHmapHandlerToMmap(const MetaDataWrapperHashMap& hmap,
                                     const std::string& filename);

// _______________________________________________________________________
IndexMetaDataMmap convertHmapMetaDataToMmap(const IndexMetaDataHmap& hmap,
                                            const std::string& filename,
                                            bool verify);

// Convert hashmap based permutation to mmap-based permutation
// Arguments:
//   permutIn: path to permutation with hash map based meta data
//   permutOut: path to File where mmap based permutation is written. This file
//            will be overwritten
//   mmap : path to file were the persistent mmap vector will be stored (will be
//     overwritten)
void convertHmapBasedPermutatationToMmap(const string& permutIn,
                                         const string& permutOut,
                                         const string& mmap,
                                         bool verify = true);

// Copy hashMap based permutation and update meta data format (add magic number)
// permutIn is read, permutOut is (over)written.
void addMagicNumberToSparseMetaDataPermutation(const string& permutIn,
                                               const string& permutOut);

// Copy the permutation data from oldPermutation to newPermutation and add the
// meta data
template <class MetaData>
void writeNewPermutation(const string& oldPermutation,
                         const string& newPermutation, const MetaData& meta);
