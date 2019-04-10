// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#include "index/Vocabulary.h"
#include "index/VocabularyGenerator.h"

int main(int argc, char** argv) {
  std::string basename = argv[1];
  size_t numFiles = atoi(argv[2]);

  VocabularyMerger m;
  m.mergeVocabulary(basename, numFiles, StringSortComparator());
}
