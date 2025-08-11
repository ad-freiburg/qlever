// Copyright 2025 University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARYIMPL_H
#define QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARYIMPL_H

#include <type_traits>

#include "index/Vocabulary.h"
#include "index/vocabulary/GeoVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "util/Log.h"
#include "util/TypeTraits.h"

// _____________________________________________________________________________
template <typename SF, typename SFN, typename... S>
requires SplitFunctionT<SF> && SplitFilenameFunctionT<SFN, sizeof...(S)>
void SplitVocabulary<SF, SFN, S...>::readFromFile(const std::string& filename) {
  auto readSingle = [](auto& vocab, const std::string& filename) {
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
  auto vocabFilenames = splitFilenameFunction_(filename);
  for (uint8_t i = 0; i < numberOfVocabs; i++) {
    std::visit([&](auto& vocab) { readSingle(vocab, vocabFilenames[i]); },
               underlying_[i]);
  }
}

// _____________________________________________________________________________
template <typename SF, typename SFN, typename... S>
requires SplitFunctionT<SF> && SplitFilenameFunctionT<SFN, sizeof...(S)>
void SplitVocabulary<SF, SFN, S...>::open(const std::string& filename) {
  auto vocabFilenames = splitFilenameFunction_(filename);
  for (uint8_t i = 0; i < numberOfVocabs; i++) {
    std::visit([&](auto& vocab) { vocab.open(vocabFilenames[i]); },
               underlying_[i]);
  }
}

// _____________________________________________________________________________
template <typename SF, typename SFN, typename... S>
requires SplitFunctionT<SF> && SplitFilenameFunctionT<SFN, sizeof...(S)>
SplitVocabulary<SF, SFN, S...>::WordWriter::WordWriter(
    const UnderlyingVocabsArray& underlyingVocabularies,
    const std::string& filename) {
  // Init all underlying word writers
  auto vocabFilenames = splitFilenameFunction_(filename);
  for (uint8_t i = 0; i < numberOfVocabs; i++) {
    underlyingWordWriters_[i] = std::visit(
        [&](auto& vocab) -> AnyUnderlyingWordWriterPtr {
          return vocab.makeDiskWriterPtr(vocabFilenames[i]);
        },
        underlyingVocabularies[i]);
  }
};

// _____________________________________________________________________________
template <typename SF, typename SFN, typename... S>
requires SplitFunctionT<SF> && SplitFilenameFunctionT<SFN, sizeof...(S)>
uint64_t SplitVocabulary<SF, SFN, S...>::WordWriter::operator()(
    std::string_view word, bool isExternal) {
  // The word will be stored in the vocabulary selected by the split
  // function. Therefore the word's index needs the marker bit(s) set
  // accordingly.
  auto splitIdx = splitFunction_(word);
  return addMarker((*underlyingWordWriters_[splitIdx])(word, isExternal),
                   splitIdx);
}

// _____________________________________________________________________________
template <typename SF, typename SFN, typename... S>
requires SplitFunctionT<SF> && SplitFilenameFunctionT<SFN, sizeof...(S)>
void SplitVocabulary<SF, SFN, S...>::WordWriter::finishImpl() {
  for (const auto& wordWriter : underlyingWordWriters_) {
    wordWriter->finish();
  }
}

// _____________________________________________________________________________
template <typename SF, typename SFN, typename... S>
requires SplitFunctionT<SF> && SplitFilenameFunctionT<SFN, sizeof...(S)>
SplitVocabulary<SF, SFN, S...>::WordWriter::~WordWriter() {
  if (!finishWasCalled()) {
    ad_utility::terminateIfThrows([this]() { this->finish(); },
                                  "Calling `finish` from the destructor of "
                                  "`SplitVocabulary`");
  }
}
// _____________________________________________________________________________
template <typename SF, typename SFN, typename... S>
requires SplitFunctionT<SF> && SplitFilenameFunctionT<SFN, sizeof...(S)>
void SplitVocabulary<SF, SFN, S...>::close() {
  for (auto& vocab : underlying_) {
    std::visit([&](auto& v) { v.close(); }, vocab);
  }
}

// _____________________________________________________________________________
template <typename SF, typename SFN, typename... S>
requires SplitFunctionT<SF> && SplitFilenameFunctionT<SFN, sizeof...(S)>
std::optional<ad_utility::GeometryInfo>
SplitVocabulary<SF, SFN, S...>::getGeoInfo(uint64_t indexWithMarker) const {
  // Visit the underlying vocabulary and retrieve the requested `GeometryInfo`
  // if it is a `GeoVocabulary`.
  const auto& vocab = underlying_[getMarker(indexWithMarker)];
  return std::visit(
      [&](const auto& v) -> std::optional<ad_utility::GeometryInfo> {
        using T = std::decay_t<decltype(v)>;
        if constexpr (ad_utility::isInstantiation<T, GeoVocabulary>) {
          return v.getGeoInfo(getVocabIndex(indexWithMarker));
        } else {
          static_assert(NeverProvidesGeometryInfo<T>);
          return std::nullopt;
        }
      },
      vocab);
}

// _____________________________________________________________________________
template <typename SF, typename SFN, typename... S>
requires SplitFunctionT<SF> && SplitFilenameFunctionT<SFN, sizeof...(S)>
bool SplitVocabulary<SF, SFN, S...>::isGeoInfoAvailable() {
  // If any of the underlying vocabularies is a `GeoVocabulary`, then this
  // `SplitVocabulary` is able to provide precomputed `GeometryInfo`. The other
  // two possibilities, `SplitVocabulary` and `PolymorphicVocabulary`, which
  // could potentially hold a `GeoVocabulary` are forbidden here due to the
  // constraints of this `SplitVocabulary` for its underlying vocabularies.
  if constexpr (ad_utility::anyIsInstantiationOf<GeoVocabulary, S...>) {
    return true;
  } else {
    // This assertion guarantees that none of the underlying vocabularies are
    // able to provide precomputed `GeometryInfo` (neither directly nor in a
    // nested fashion). For more details and if it fails, see the definition of
    // this concept in `VocabularyConstraints.h`.
    static_assert(AllNeverProvideGeometryInfo<S...>);
    return false;
  }
}

#endif  // QLEVER_SRC_INDEX_VOCABULARY_SPLITVOCABULARYIMPL_H
