// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_TEST_WORDSANDDOCSFILELINECREATOR_H
#define QLEVER_TEST_WORDSANDDOCSFILELINECREATOR_H

#include <absl/strings/str_cat.h>

constexpr std::string_view inlineSeparator = "\t";
constexpr std::string_view lineSeparator = "\n";

inline std::string createWordsFileLineAsString(std::string_view word,
                                               bool isEntity, size_t contextId,
                                               size_t score) {
  return absl::StrCat(word, inlineSeparator, isEntity, inlineSeparator,
                      contextId, inlineSeparator, score, lineSeparator);
};

inline std::string createDocsFileLineAsString(size_t docId,
                                              std::string_view docContent) {
  return absl::StrCat(docId, inlineSeparator, docContent, lineSeparator);
};

#endif  // QLEVER_TEST_WORDSANDDOCSFILELINECREATOR_H
