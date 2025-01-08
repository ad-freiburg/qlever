// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#pragma once

inline std::string inlineSeparator = "\t";
inline std::string lineSeparator = "\n";

inline std::string createWordsFileLine(std::string word, bool isEntity,
                                       size_t contextId, size_t score) {
  return word + inlineSeparator + (isEntity ? "1" : "0") + inlineSeparator +
         std::to_string(contextId) + inlineSeparator + std::to_string(score) +
         lineSeparator;
};

inline std::string createDocsFileLine(size_t docId, std::string docContent) {
  return std::to_string(docId) + inlineSeparator + docContent + lineSeparator;
};
