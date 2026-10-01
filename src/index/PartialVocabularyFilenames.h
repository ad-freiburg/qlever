// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_PARTIALVOCABULARYFILENAMES_H
#define QLEVER_SRC_INDEX_PARTIALVOCABULARYFILENAMES_H

#include <absl/strings/str_cat.h>

#include <cstddef>
#include <string>
#include <string_view>

#include "backports/algorithm.h"
#include "index/ConstantsIndexBuilding.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/Views.h"

// The names of the temporary files that the index builder creates for each of
// the partial vocabularies (see `IndexImpl::buildPartialVocabularies`): the
// words of the partial vocabulary, the mapping from its partial IDs to the
// global IDs, and the triples that were mapped using it. All of them are
// numbered by the index of the partial vocabulary they belong to, so the files
// with the same index always belong together.
//
// Each kind of file has a function that yields the file of a single partial
// vocabulary, for the callers that only need one of them. The kinds that are
// also consumed as a whole additionally have a function that yields the files
// of all the partial vocabularies as a lazy range. Those ranges are
// type-erased and self-contained (in particular, they own the `basename`), so
// that their consumers are completely oblivious of how the files are named.

// The file that holds the words of the `idx`-th partial vocabulary.
inline std::string partialVocabularyWordsFilename(std::string_view basename,
                                                  size_t idx) {
  return absl::StrCat(basename, PARTIAL_VOCAB_WORDS_INFIX, idx);
}

// The file that holds the mapping from the partial IDs of the `idx`-th partial
// vocabulary to the global IDs.
inline std::string partialVocabularyIdMapFilename(std::string_view basename,
                                                  size_t idx) {
  return absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, idx);
}

// The file that holds the triples that were mapped using the `idx`-th partial
// vocabulary.
inline std::string unsortedTriplesFilename(std::string_view basename,
                                           size_t idx) {
  return absl::StrCat(basename, UNSORTED_TRIPLES_INFIX, idx, ".dat");
}

namespace partialVocabularyFilenames::detail {
// The filenames that `makeFilename` yields for each of the
// `numPartialVocabularies` partial vocabularies. NOTE: The returned range owns
// the `basename`, so it is self-contained and outliving its arguments is fine.
template <typename F>
ad_utility::InputRangeTypeErased<std::string> makeFilenames(
    std::string_view basename, size_t numPartialVocabularies, F makeFilename) {
  return ad_utility::InputRangeTypeErased<std::string>{
      ad_utility::integerRange(numPartialVocabularies) |
      ql::views::transform(
          [basename = std::string{basename}, makeFilename](size_t idx) {
            return makeFilename(basename, idx);
          })};
}
}  // namespace partialVocabularyFilenames::detail

// The words files of all the `numPartialVocabularies` partial vocabularies.
inline ad_utility::InputRangeTypeErased<std::string>
partialVocabularyWordsFilenames(std::string_view basename,
                                size_t numPartialVocabularies) {
  return partialVocabularyFilenames::detail::makeFilenames(
      basename, numPartialVocabularies, partialVocabularyWordsFilename);
}

// The ID map files of all the `numPartialVocabularies` partial vocabularies.
inline ad_utility::InputRangeTypeErased<std::string>
partialVocabularyIdMapFilenames(std::string_view basename,
                                size_t numPartialVocabularies) {
  return partialVocabularyFilenames::detail::makeFilenames(
      basename, numPartialVocabularies, partialVocabularyIdMapFilename);
}

// Delete the words files of all the `numPartialVocabularies` partial
// vocabularies; they are not needed anymore once the vocabulary has been
// merged. The `deleteFile` callback does the actual deletion, because the
// caller decides whether temporary files are kept (see
// `IndexImpl::deleteTemporaryFile`).
template <typename F>
void deletePartialVocabularyWordsFiles(std::string_view basename,
                                       size_t numPartialVocabularies,
                                       const F& deleteFile) {
  AD_LOG_DEBUG << "Removing temporary files ..." << std::endl;
  for (const std::string& filename :
       partialVocabularyWordsFilenames(basename, numPartialVocabularies)) {
    deleteFile(filename);
  }
}

#endif  // QLEVER_SRC_INDEX_PARTIALVOCABULARYFILENAMES_H
