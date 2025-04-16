// Copyright 2025 University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "index/vocabulary/SplitVocabulary.h"

#include "index/Vocabulary.h"
#include "util/Exception.h"
#include "util/Log.h"

// _____________________________________________________________________________
template <class M, class S, const auto& SF, const auto& SFN>
uint64_t SplitVocabulary<M, S, SF, SFN>::makeSpecialVocabIndex(
    uint64_t vocabIndex) {
  AD_CORRECTNESS_CHECK(vocabIndex < maxVocabIndex);
  return vocabIndex | specialVocabMarker;
}

// _____________________________________________________________________________
template <class M, class S, const auto& SF, const auto& SFN>
void SplitVocabulary<M, S, SF, SFN>::readFromFile(const string& filename) {
  auto readSingle = [](auto& vocab, const string& filename) {
    LOG(INFO) << "Reading vocabulary from file " << filename << " ..."
              << std::endl;
    vocab.close();
    vocab.open(filename);
    LOG(INFO) << "Done, number of words: " << vocab.size() << std::endl;
  };

  auto [fnMain, fnSpecial] = SFN(filename);
  readSingle(underlyingMain_, fnMain);
  readSingle(underlyingSpecial_, fnSpecial);
}

// _____________________________________________________________________________
template <class M, class S, const auto& SF, const auto& SFN>
decltype(auto) SplitVocabulary<M, S, SF, SFN>::operator[](uint64_t idx) const {
  // Check marker bit to determine which vocabulary to use
  if (idx & specialVocabMarker) {
    // The requested word is stored in the special vocabulary
    uint64_t unmarkedIdx = idx & specialVocabIndexMask;
    return underlyingSpecial_[unmarkedIdx];
  } else {
    // The requested word is stored in the vocabulary for normal words
    AD_CONTRACT_CHECK(idx <= maxVocabIndex);
    return underlyingMain_[idx];
  }
}

// _____________________________________________________________________________
template <class M, class S, const auto& SF, const auto& SFN>
SplitVocabulary<M, S, SF, SFN>::WordWriter::WordWriter(
    const SplitVocabulary<M, S, SF, SFN>& vocabulary,
    const std::string& filename)
    : underlyingWordWriter_{vocabulary.getUnderlyingVocabulary().makeDiskWriter(
          SFN(filename).at(0))},
      underlyingSpecialWordWriter_{
          vocabulary.getUnderlyingSpecialVocabulary().makeDiskWriter(
              SFN(filename).at(1))} {};

// _____________________________________________________________________________
template <class M, class S, const auto& SF, const auto& SFN>
uint64_t SplitVocabulary<M, S, SF, SFN>::WordWriter::operator()(
    std::string_view word, bool isExternal) {
  if (SF(word)) {
    // The word to be stored in the vocabulary is selected by the split function
    // to go to the special vocabulary. It needs an index with the marker bit
    // set to 1.
    return makeSpecialVocabIndex(
        underlyingSpecialWordWriter_(word, isExternal));
  } else {
    // We have any other word: it goes to the normal vocabulary.
    auto index = underlyingWordWriter_(word, isExternal);
    AD_CONTRACT_CHECK(index <= maxVocabIndex);
    return index;
  }
}

// _____________________________________________________________________________
template <class M, class S, const auto& SF, const auto& SFN>
void SplitVocabulary<M, S, SF, SFN>::WordWriter::finish() {
  underlyingWordWriter_.finish();
  underlyingSpecialWordWriter_.finish();
}

// _____________________________________________________________________________
template <class M, class S, const auto& SF, const auto& SFN>
void SplitVocabulary<M, S, SF, SFN>::createFromSet(
    const ad_utility::HashSet<std::string>& set, const std::string& filename) {
  // TODO
  LOG(DEBUG) << "BEGIN Vocabulary::createFromSet" << std::endl;
  underlyingMain_.close();
  underlyingSpecial_.close();

  // Split words depending on whether they should be stored in the normal or
  // geometry vocabulary
  std::vector<std::string> words;
  std::vector<std::string> geoWords;
  for (const auto& word : set) {
    if (SF(word)) {
      geoWords.push_back(word);
    } else {
      words.push_back(word);
    }
  }

  auto totalComparison = [this](const auto& a, const auto& b) {
    return getCaseComparator()(a, b, SortLevel::TOTAL);
  };
  auto buildSingle = [totalComparison](VocabularyWithUnicodeComparator& vocab,
                                       std::vector<std::string>& words,
                                       const std::string& filename) {
    std::sort(begin(words), end(words), totalComparison);
    vocab.build(words, filename);
  };
  buildSingle(vocabulary_, words, filename);
  buildSingle(geoVocabulary_, geoWords, filename + geoVocabSuffix);

  LOG(DEBUG) << "END Vocabulary::createFromSet" << std::endl;
}

// _____________________________________________________________________________
template <class M, class S, const auto& SF, const auto& SFN>
bool SplitVocabulary<M, S, SF, SFN>::getId(std::string_view word,
                                           uint64_t* idx) const {
  // Helper lambda to lookup a the word in a given vocabulary.
  auto checkWord = [&word, &idx](const auto& vocab) -> bool {
    // We need the TOTAL level because we want the unique word.
    auto wordAndIndex = vocab.lower_bound(word, SortLevel::TOTAL);
    if (wordAndIndex.isEnd()) {
      return false;
    }
    *idx = wordAndIndex.index();
    return wordAndIndex.word() == word;
  };

  // Check if the word is in the regular non-geometry vocabulary
  if (checkWord(underlyingMain_)) {
    return true;
  }

  // Not found in regular vocabulary: test if it is in the geometry vocabulary
  bool res = checkWord(underlyingSpecial_);
  // Index with special marker bit for geometry word
  *idx |= specialVocabMarker;
  return res;
};
