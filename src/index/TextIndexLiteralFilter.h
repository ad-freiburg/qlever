// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_TRIPLEINTEXTINDEX_H
#define QLEVER_SRC_INDEX_TRIPLEINTEXTINDEX_H

#include <string>

#include "parser/RdfParser.h"

class TextIndexLiteralFilter {
 public:
  enum struct FilterType { AcceptMatching, DeclineMatching };

  TextIndexLiteralFilter()
      : regex_{std::make_unique<RE2>("(?s).*")},
        filterType_{FilterType::AcceptMatching} {};

  /**
   * @brief Class to determine whether the literal of a triple should be part of
   *        the text index or not.
   * @param regex The regex that is used to determine whether a literal should
   *              be part of the text index or not. This is used on the
   *              predicate.
   * @param filterType If set to AcceptMatching, the matching regex cases will
   *                   be added to the text index. If set to DeclineMatching,
   *                   the matching regex cases will not be added but everything
   *                   else.
   * @param addAllLiterals If set to true this can later be used to not only
   *        add objects of triples but all literals.
   */
  explicit TextIndexLiteralFilter(std::string regex, FilterType filterType,
                                  bool addAllLiterals)
      : regex_{std::make_unique<RE2>(regex, RE2::Quiet)},
        filterType_{filterType},
        addAllLiterals_(addAllLiterals) {
    if (!regex_->ok()) {
      throw std::runtime_error{
          absl::StrCat("The regex supposed to filter predicates for which the "
                       "objects are stored in the text index was \"",
                       regex_->pattern(),
                       "\". This is not supported by QLever (which uses "
                       "Google's RE2 library); "
                       "the error from RE2 is: ",
                       regex_->error())};
    }
  }

  // Computes inTextIndexMap_ depending on the values for regex, whitelist and
  // addAllLiterals. Later the values can be retrieved using the operator().
  std::tuple<bool, bool, bool> computeInTextIndexMap(const TripleComponent& s,
                                                     const TripleComponent& p,
                                                     const TripleComponent& o);

  bool addAllLiterals() const { return addAllLiterals_; }

  bool isWhiteList() const { return filterType_ == FilterType::AcceptMatching; }

 private:
  // The regex string used to do the comparison
  std::unique_ptr<RE2> regex_ = nullptr;
  // Determine whether the regex should act as whitelist or blacklist
  FilterType filterType_ = FilterType::AcceptMatching;
  // True when all literals not only objects should be added
  bool addAllLiterals_ = false;
};

#endif  // QLEVER_SRC_INDEX_TRIPLEINTEXTINDEX_H
