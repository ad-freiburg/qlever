// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
// Only performs the "mergeVocabulary" step of the IndexBuilder pipeline
// Can be used e.g. for benchmarking this step to develop faster IndexBuilders.

#include "index/Vocabulary.h"
#include "index/VocabularyGenerator.h"

// ____________________________________________________________________________________________________
int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr
        << "Usage: " << argv[0]
        << "<basename of index> <number of partial vocabulary files to merge>";
  }
  std::string basename = argv[1];
  size_t numFiles = atoi(argv[2]);

  VocabularyMerger m;
  std::ofstream file(basename + ".vocabulary");
  AD_CHECK(file.is_open());
  auto internalVocabularyAction = [&file](const auto& word) {
    file << RdfEscaping::escapeNewlinesAndBackslashes(word) << '\n';
  };
  m.mergeVocabulary(basename, numFiles, TripleComponentComparator(),
                    internalVocabularyAction);
}
