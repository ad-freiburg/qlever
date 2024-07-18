// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
// Only performs the "mergeVocabulary" step of the IndexBuilder pipeline
// Can be used e.g. for benchmarking this step to develop faster IndexBuilders.

#include "index/Vocabulary.h"
#include "index/VocabularyMerger.h"

// ____________________________________________________________________________________________________
int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr
        << "Usage: " << argv[0]
        << "<basename of index> <number of partial vocabulary files to merge>";
  }
  std::string basename = argv[1];
  size_t numFiles = atoi(argv[2]);

  auto file = ad_utility::makeOfstream(basename + VOCAB_SUFFIX);
  auto wordCallback = [&file](const auto& word,
                              [[maybe_unused]] bool isExternal) {
    file << RdfEscaping::escapeNewlinesAndBackslashes(word) << '\n';
  };
  VocabularyOnDisk vocab;
  ad_utility::vocabulary_merger::mergeVocabulary(
      basename, numFiles, TripleComponentComparator(), wordCallback, 4_GB);
}
