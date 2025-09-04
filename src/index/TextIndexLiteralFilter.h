// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_TEXTINDEXLITERALFILTER_H
#define QLEVER_SRC_INDEX_TEXTINDEXLITERALFILTER_H

#include "index/TextIndexLiteralConfiguration.h"
#include "parser/RdfParser.h"
/**
 * @brief Class to determine whether the literal of a triple should be part of
 *        the text index or not.
 * @details See `TextIndexLiteralConfiguration` for details.
 */
class TextIndexLiteralFilter {
 public:
  // Empty constructor gives back a filter that adds every literal to the text
  // index
  TextIndexLiteralFilter();

  explicit TextIndexLiteralFilter(const TextIndexLiteralConfiguration& config);

  // Computes inTextIndexMap_ depending on the values for regex, whitelist and
  // addAllLiterals. The bools determine whether the subject, predicate and
  // object should be added to the text index.
  std::tuple<bool, bool, bool> computeInTextIndexMap(
      const TripleComponent& s, const TripleComponent& p,
      const TripleComponent& o) const;

  // Does the same as `computeInTextIndexMap` but only returns whether the
  // object should be added to the text index.
  bool shouldObjectBeInTextIndex(const TripleComponent& p,
                                 const TripleComponent& o) const;

  bool addAllLiterals() const { return addAllLiterals_; }

  bool isWhiteList() const {
    return filterType_ == LiteralFilterType::AcceptMatching;
  }

 private:
  // The regex string used to do the comparison
  std::unique_ptr<RE2> regex_;
  // Determine whether the regex should act as whitelist or blacklist
  LiteralFilterType filterType_;
  // True when all literals not only objects should be added
  bool addAllLiterals_;
};

#endif  // QLEVER_SRC_INDEX_TEXTINDEXLITERALFILTER_H
