// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_TEXTINDEXLITERALCONFIGURATION_H
#define QLEVER_SRC_INDEX_TEXTINDEXLITERALCONFIGURATION_H

#include <string>

enum struct LiteralFilterType { AcceptMatching, DeclineMatching };

/**
 * @brief This struct is the configuration for the `TextIndexLiteralFilter`
 *        which is used to determine which literals should be added to the
 *        text index
 *
 * @param predicateRegex_ This string will be used as RE2 Regex and applied on
 *                        the predicates of triples during index building to
 *                        decide whether objects should be marked as 'add to
 *                        text index'. Objects are only marked as 'add to text
 *                        index' if they are literals.
 * @param isWhitelistOrBlacklist_ If set to `AcceptMatching`, the object of a
 *                                triple is only marked as 'add to text index'
 *                                if `predicateRegex_` matches the predicate.
 *                                If set to `DeclineMatching`, the object of a
 *                                triple is only marked as 'add to text index'
 *                                if `predicateRegex_` doesn't match the
 *                                predicate.
 * @param addAllLiterals_ If set to true all object literals are marked as
 *                        'add to text index'.
 */
struct TextIndexLiteralConfiguration {
  std::string predicateRegex_ = "";
  LiteralFilterType isWhitelistOrBlacklist_ = LiteralFilterType::AcceptMatching;
  bool addAllLiterals_ = true;
};

#endif  // QLEVER_SRC_INDEX_TEXTINDEXLITERALCONFIGURATION_H
