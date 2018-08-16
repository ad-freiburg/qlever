// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)
//
#include <array>
#include <iostream>
#include "./global/Constants.h"
#include "./index/MetaDataConverter.h"
#include "./util/File.h"

// _________________________________________________________
int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: ./MetaDataConverterMain <indexPrefix>\n";
    exit(1);
  }
  std::string in = argv[1];
  std::array<std::string, 4> sparseNames{".pso", ".pos", ".spo", ".sop"};
  for (const auto& n : sparseNames) {
    std::string permutName = in + ".index" + n;
    if (!ad_utility::File::exists(permutName)) {
      std::cerr << "Permutation file " << permutName
                << " was not found. Maybe not all permutations were built for "
                   "this index. Skipping\n";
      continue;
    }
    addMagicNumberToSparseMetaDataPermutation(permutName,
                                              permutName + ".converted");
  }

  std::array<std::string, 2> denseNames{".osp", ".ops"};
  for (const auto& n : denseNames) {
    std::string permutName = in + ".index" + n;
    if (!ad_utility::File::exists(permutName)) {
      std::cerr << "Permutation file " << permutName
                << " was not found. Maybe not all permutations were built for "
                   "this index. Skipping\n";
      continue;
    }
    convertHmapBasedPermutatationToMmap(permutName, permutName + ".converted",
                                        permutName + MMAP_FILE_SUFFIX);
  }
}
