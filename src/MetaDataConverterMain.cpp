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
// Opens an index from disk. Determines whether this index was built by an older
// QLever version and has to be updated in order to use it (efficiently or at
// all) with the current QLever version. Will NOT overwrite existing files but
// create new files with a .converted suffix which has  to be manually removed
// to make the index work. It is highly recommended to backup the original index
// before overwriting it like this.
//
// This converter prints detailed information about which files were created and
// which files have to be renamed in ordere to  complete the index update
int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: ./MetaDataConverterMain <indexPrefix>\n";
    exit(1);
  }
  std::string in = argv[1];
  std::array<std::string, 2> sparseNames{".pso", ".pos"};
  for (const auto& n : sparseNames) {
    std::string permutName = in + ".index" + n;
    if (!ad_utility::File::exists(permutName)) {
      std::cerr << "Permutation file " << permutName
                << " was not found. Maybe not all permutations were built for "
                   "this index. Skipping\n";
      continue;
    }
    convertPermutationToHmap(permutName, permutName + ".converted");
  }

  std::array<std::string, 4> denseNames{".spo", ".sop", ".osp", ".ops"};
  for (const auto& n : denseNames) {
    std::string permutName = in + ".index" + n;
    if (!ad_utility::File::exists(permutName)) {
      std::cerr << "Permutation file " << permutName
                << " was not found. Maybe not all permutations were built for "
                   "this index. Skipping\n";
      continue;
    }
    convertPermutationToMmap(permutName, permutName + ".converted",
                             permutName + MMAP_FILE_SUFFIX);
  }
  CompressVocabAndCreateConfigurationFile(in);
}
