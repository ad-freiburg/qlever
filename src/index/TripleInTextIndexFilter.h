// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef TRIPLEINTEXTINDEX_H
#define TRIPLEINTEXTINDEX_H

#include <string>

#include "parser/RdfParser.h"

class TripleInTextIndexFilter {
 public:
  TripleInTextIndexFilter() : regex_{"(?s).*"}, isWhitelist_{true} {};

  /**
   * @brief Class to determine whether the literal of a triple should be part of
   *        the text index or not.
   * @param regex The regex that is used to determine whether a literal should
   *              be part of the text index or not. This is used on the
   *              predicate.
   * @param whitelist If set to true the matching regex cases will be added to
   *                  the text index. If set to falls the matching regex cases
   *                  will not be added but everything else.
   */
  TripleInTextIndexFilter(std::string regex, bool whitelist = true)
      : regex_{std::move(regex)}, isWhitelist_{whitelist} {
    if (const RE2 reg{regex_, RE2::Quiet}; !reg.ok()) {
      throw std::runtime_error{absl::StrCat(
          "The regex \"", regex_,
          "\" is not supported by QLever (which uses Google's RE2 library); "
          "the error from RE2 is: ",
          reg.error())};
    }
  }

  // Returns true iff object of triple is literal or IRI and should be added to
  // the text index depending on the given regex and whitelist parameter. The
  // regex looks for a partial match.
  bool operator()(const TripleComponent& p, const TripleComponent& o) const;

 private:
  // The regex string used to do the comparison
  std::string regex_;
  // Determine whether the regex should act as whitelist or blacklist
  bool isWhitelist_;
};

#endif  // TRIPLEINTEXTINDEX_H
