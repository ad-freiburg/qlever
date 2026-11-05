// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "index/vocabulary/VocabularyInMemory.h"

#include <memory>
#include <string_view>
#include <vector>

#include "backports/algorithm.h"
#include "util/Exception.h"

using std::string;

// _____________________________________________________________________________
void VocabularyInMemory::open(const string& fileName) {
  AD_LOG_INFO << "Reading vocabulary from file " << fileName << " ..."
              << std::endl;
  Words words;
  ad_utility::serialization::FileReadSerializer file(fileName);
  file >> words;
  words_ = std::make_shared<const Words>(std::move(words));
  AD_LOG_INFO << "Done, number of words: " << size() << std::endl;
}

// _____________________________________________________________________________
void VocabularyInMemory::writeToFile(const string& fileName) const {
  AD_LOG_INFO << "Writing vocabulary to file " << fileName << " ..."
              << std::endl;
  ad_utility::serialization::FileWriteSerializer file{fileName};
  file << words();
  AD_LOG_INFO << "Done, number of words: " << words().size() << std::endl;
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyInMemory::lookupBatch(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  return SharedWordsVocabBatchLookupData::fromViews(
      words_,
      ::ranges::to_vector(indices | ql::views::transform([this](size_t idx) {
                            AD_CONTRACT_CHECK(idx < size());
                            return words()[idx];
                          })));
}
