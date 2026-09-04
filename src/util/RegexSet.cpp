// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/RegexSet.h"

#include <absl/strings/str_cat.h>
#include <re2/re2.h>
#include <re2/set.h>

#include <stdexcept>

#include "util/Exception.h"

namespace ad_utility {

// All the state of a `RegexSet`. `ANCHOR_BOTH` makes the matching a full match
// (as for `RE2::FullMatch`), and `Quiet` suppresses the logging of `RE2` for
// invalid regexes, which we report as exceptions instead (see the constructor
// of `RegexSet`).
struct RegexSet::Impl {
  std::vector<std::string> regexesAsStrings_;
  re2::RE2::Set set_{re2::RE2::Quiet, re2::RE2::ANCHOR_BOTH};

  explicit Impl(std::vector<std::string> regexesAsStrings)
      : regexesAsStrings_{std::move(regexesAsStrings)} {}
};

// _____________________________________________________________________________
RegexSet::RegexSet(std::vector<std::string> regexesAsStrings,
                   std::string_view description) {
  if (regexesAsStrings.empty()) {
    return;
  }
  auto impl = std::make_shared<Impl>(std::move(regexesAsStrings));
  // NOTE: `RE2::Set::Add` does not throw for a regex that is not valid, but
  // returns a negative index and stores a descriptive message in its second
  // argument, which we turn into a user-readable exception here.
  for (const std::string& regex : impl->regexesAsStrings_) {
    std::string error;
    if (impl->set_.Add(regex, &error) < 0) {
      throw std::runtime_error{absl::StrCat(
          "The regex \"", regex, "\" ", description,
          " is not a valid regular expression (as understood by Google's RE2 "
          "library): ",
          error)};
    }
  }
  // NOTE: Each of the regexes has been accepted by `Add` above, so compiling
  // them into a single automaton can only fail if that automaton exceeds the
  // memory budget of `RE2`, which in practice requires an absurd number of (or
  // absurdly complex) regexes.
  bool compilationSucceeded = impl->set_.Compile();
  AD_CORRECTNESS_CHECK(compilationSucceeded, "The ",
                       impl->regexesAsStrings_.size(), " regexes ", description,
                       " are each a valid regular expression, but they could "
                       "not be compiled into a single automaton (as understood "
                       "by Google's RE2 library). Try using fewer or simpler "
                       "regexes");
  impl_ = std::move(impl);
}

// _____________________________________________________________________________
bool RegexSet::matchesAny(std::string_view word) const {
  if (!impl_) {
    return false;
  }
  re2::RE2::Set::ErrorInfo errorInfo{re2::RE2::Set::kNoError};
  bool result = impl_->set_.Match(word, nullptr, &errorInfo);
  // A failed match may also mean that the matching itself failed (for example
  // because the automaton ran out of memory), which we must not silently report
  // as "no match".
  bool matchingSucceeded = result || errorInfo.kind == re2::RE2::Set::kNoError;
  AD_CORRECTNESS_CHECK(matchingSucceeded, "Matching the ",
                       impl_->regexesAsStrings_.size(),
                       " regexes against a string failed (RE2 reported the "
                       "error kind ",
                       static_cast<int>(errorInfo.kind), ")");
  return result;
}

// _____________________________________________________________________________
const std::vector<std::string>& RegexSet::regexesAsStrings() const {
  static const std::vector<std::string> emptyRegexes;
  return impl_ ? impl_->regexesAsStrings_ : emptyRegexes;
}

}  // namespace ad_utility
