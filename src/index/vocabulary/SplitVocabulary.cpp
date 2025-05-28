// Copyright 2025 University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "index/vocabulary/SplitVocabulary.h"

#include "concepts/concepts.hpp"
#include "index/Vocabulary.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Exception.h"
#include "util/Log.h"

// _____________________________________________________________________________
CPP_template(const auto& SF, const auto& SFN, class... S)(
    requires SplitFunctionT<SF> CPP_and SplitFilenameFunctionT<
        SFN, sizeof...(S)>) void SplitVocabulary<SF, SFN, S...>::
    readFromFile(const string& filename) {
  auto readSingle = [](auto& vocab, const string& filename) {
    LOG(INFO) << "Reading vocabulary from file " << filename << " ..."
              << std::endl;
    vocab.close();
    vocab.open(filename);

    if constexpr (std::is_same_v<decltype(vocab),
                                 detail::UnderlyingVocabRdfsVocabulary>) {
      const auto& internalExternalVocab =
          vocab.getUnderlyingVocabulary().getUnderlyingVocabulary();
      LOG(INFO) << "Done, number of words: "
                << internalExternalVocab.internalVocab().size() << std::endl;
      LOG(INFO) << "Number of words in external vocabulary: "
                << internalExternalVocab.externalVocab().size() << std::endl;
    } else {
      LOG(INFO) << "Done, number of words: " << vocab.size() << std::endl;
    }
  };

  // Make filenames and read each underlying vocabulary
  auto vocabFilenames = SFN(filename);
  for (uint8_t i = 0; i < numberOfVocabs; i++) {
    std::visit([&](auto& vocab) { readSingle(vocab, vocabFilenames[i]); },
               underlying_[i]);
  }
}

// _____________________________________________________________________________
CPP_template(const auto& SF, const auto& SFN, class... S)(
    requires SplitFunctionT<SF> CPP_and SplitFilenameFunctionT<
        SFN, sizeof...(S)>) void SplitVocabulary<SF, SFN,
                                                 S...>::open(const string&
                                                                 filename) {
  auto vocabFilenames = SFN(filename);
  for (uint8_t i = 0; i < numberOfVocabs; i++) {
    std::visit([&](auto& vocab) { vocab.open(vocabFilenames[i]); },
               underlying_[i]);
  }
}

// _____________________________________________________________________________
CPP_template(const auto& SF, const auto& SFN,
             class... S)(requires SplitFunctionT<SF> CPP_and
                             SplitFilenameFunctionT<SFN, sizeof...(S)>)
    SplitVocabulary<SF, SFN, S...>::WordWriter::WordWriter(
        const UnderlyingVocabsArray& underlyingVocabularies,
        const std::string& filename) {
  // Init all underlying word writers
  auto vocabFilenames = SFN(filename);
  for (uint8_t i = 0; i < numberOfVocabs; i++) {
    underlyingWordWriters_[i] = std::visit(
        [&](auto& vocab) -> AnyUnderlyingWordWriterPtr {
          return vocab.makeDiskWriterPtr(vocabFilenames[i]);
        },
        underlyingVocabularies[i]);
  }
};

// _____________________________________________________________________________
CPP_template(const auto& SF, const auto& SFN,
             class... S)(requires SplitFunctionT<SF> CPP_and
                             SplitFilenameFunctionT<SFN, sizeof...(S)>) uint64_t
    SplitVocabulary<SF, SFN, S...>::WordWriter::operator()(
        std::string_view word, bool isExternal) {
  // The word will be stored in the vocabulary selected by the split
  // function. Therefore the word's index needs the marker bit(s) set
  // accordingly.
  auto splitIdx = SF(word);
  return addMarker((*underlyingWordWriters_[splitIdx])(word, isExternal),
                   splitIdx);
}

// _____________________________________________________________________________
CPP_template(const auto& SF, const auto& SFN, class... S)(
    requires SplitFunctionT<SF> CPP_and SplitFilenameFunctionT<
        SFN, sizeof...(S)>) void SplitVocabulary<SF, SFN, S...>::WordWriter::
    finishImpl() {
  for (const auto& wordWriter : underlyingWordWriters_) {
    wordWriter->finish();
  }
}

// _____________________________________________________________________________
CPP_template(const auto& SF, const auto& SFN, class... S)(
    requires SplitFunctionT<SF> CPP_and SplitFilenameFunctionT<
        SFN, sizeof...(S)>) void SplitVocabulary<SF, SFN, S...>::close() {
  for (auto& vocab : underlying_) {
    std::visit([&](auto& v) { v.close(); }, vocab);
  }
}

// Explicit template instantiations
template class SplitVocabulary<
    geoSplitFunc, geoFilenameFunc,
    CompressedVocabulary<VocabularyInternalExternal>,
    CompressedVocabulary<VocabularyInternalExternal>>;
template class SplitVocabulary<geoSplitFunc, geoFilenameFunc,
                               VocabularyInMemory, VocabularyInMemory>;
