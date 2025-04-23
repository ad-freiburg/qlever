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
CPP_template(class M, class S, const auto& SF, const auto& SFN)(
    requires SplitFunctionT<SF> CPP_and SplitFilenameFunctionT<SFN>) uint64_t
    SplitVocabulary<M, S, SF, SFN>::makeSpecialVocabIndex(uint64_t vocabIndex) {
  AD_CORRECTNESS_CHECK(vocabIndex < maxVocabIndex);
  return vocabIndex | specialVocabMarker;
}

// _____________________________________________________________________________
CPP_template(class M, class S, const auto& SF, const auto& SFN)(
    requires SplitFunctionT<SF> CPP_and SplitFilenameFunctionT<
        SFN>) void SplitVocabulary<M, S, SF, SFN>::readFromFile(const string&
                                                                    filename) {
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

  auto [fnMain, fnSpecial] = SFN(filename);
  readSingle(underlyingMain_, fnMain);
  readSingle(underlyingSpecial_, fnSpecial);
}

// _____________________________________________________________________________
CPP_template(class M, class S, const auto& SF, const auto& SFN)(
    requires SplitFunctionT<SF> CPP_and SplitFilenameFunctionT<
        SFN>) void SplitVocabulary<M, S, SF, SFN>::open(const string&
                                                            filename) {
  auto [fnMain, fnSpecial] = SFN(filename);
  underlyingMain_.open(fnMain);
  underlyingSpecial_.open(fnSpecial);
}

// _____________________________________________________________________________
CPP_template(class M, class S, const auto& SF, const auto& SFN)(
    requires SplitFunctionT<SF> CPP_and SplitFilenameFunctionT<SFN>)
    SplitVocabulary<M, S, SF, SFN>::WordWriter::WordWriter(
        const M& mainVocabulary, const S& specialVocabulary,
        const std::string& filename)
    : underlyingWordWriter_{mainVocabulary.makeDiskWriterPtr(
          SFN(filename).at(0))},
      underlyingSpecialWordWriter_{
          specialVocabulary.makeDiskWriterPtr(SFN(filename).at(1))} {};

// _____________________________________________________________________________
CPP_template(class M, class S, const auto& SF, const auto& SFN)(
    requires SplitFunctionT<SF> CPP_and SplitFilenameFunctionT<SFN>) uint64_t
    SplitVocabulary<M, S, SF, SFN>::WordWriter::operator()(
        std::string_view word, bool isExternal) {
  if (SF(word)) {
    // The word to be stored in the vocabulary is selected by the split function
    // to go to the special vocabulary. It needs an index with the marker bit
    // set to 1.
    return makeSpecialVocabIndex(
        (*underlyingSpecialWordWriter_)(word, isExternal));
  } else {
    // We have any other word: it goes to the normal vocabulary.
    auto index = (*underlyingWordWriter_)(word, isExternal);
    AD_CONTRACT_CHECK(index <= maxVocabIndex);
    return index;
  }
}

// _____________________________________________________________________________
CPP_template(class M, class S, const auto& SF, const auto& SFN)(
    requires SplitFunctionT<SF> CPP_and SplitFilenameFunctionT<
        SFN>) void SplitVocabulary<M, S, SF, SFN>::WordWriter::finish() {
  underlyingWordWriter_->finish();
  underlyingSpecialWordWriter_->finish();
}

// _____________________________________________________________________________
CPP_template(class M, class S, const auto& SF, const auto& SFN)(
    requires SplitFunctionT<SF> CPP_and SplitFilenameFunctionT<
        SFN>) void SplitVocabulary<M, S, SF, SFN>::close() {
  underlyingMain_.close();
  underlyingSpecial_.close();
}

// Explicit template instantiations
template class SplitVocabulary<CompressedVocabulary<VocabularyInternalExternal>,
                               CompressedVocabulary<VocabularyInternalExternal>,
                               geoSplitFunc, geoFilenameFunc>;
template class SplitVocabulary<VocabularyInMemory, VocabularyInMemory,
                               geoSplitFunc, geoFilenameFunc>;
