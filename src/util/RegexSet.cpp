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

// The compiled automaton for all the patterns of a `RegexSet`. `ANCHOR_BOTH`
// makes the matching a full match (as for `RE2::FullMatch`), and `Quiet`
// suppresses the logging of `RE2` for invalid patterns, which we report as
// exceptions instead (see `throwCompilationError`).
struct RegexSet::Impl {
  re2::RE2::Set set_{re2::RE2::Quiet, re2::RE2::ANCHOR_BOTH};
};

// _____________________________________________________________________________
RegexSet::RegexSet(std::vector<std::string> patterns,
                   std::string_view description)
    : patterns_{std::move(patterns)} {
  if (patterns_.empty()) {
    return;
  }
  auto impl = std::make_shared<Impl>();
  // NOTE: `RE2::Set` reports an invalid pattern by returning a negative index
  // from `Add`, and a failure to compile all the patterns into a single
  // automaton by returning `false` from `Compile`. In both cases we compile the
  // patterns one by one to obtain a message that names the offending pattern.
  for (const std::string& pattern : patterns_) {
    if (impl->set_.Add(pattern, nullptr) < 0) {
      throwCompilationError(description);
    }
  }
  if (!impl->set_.Compile()) {
    throwCompilationError(description);
  }
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
  if (!result && errorInfo.kind != re2::RE2::Set::kNoError) {
    AD_THROW(absl::StrCat("Matching the ", patterns_.size(),
                          " regexes against a string failed (RE2 reported the "
                          "error kind ",
                          static_cast<int>(errorInfo.kind), ")"));
  }
  return result;
}

// _____________________________________________________________________________
void RegexSet::throwCompilationError(std::string_view description) const {
  for (const std::string& pattern : patterns_) {
    // `RE2` does not throw for an invalid pattern but stores an error state,
    // which we turn into a user-readable exception here.
    re2::RE2 singleRegex{pattern, re2::RE2::Quiet};
    if (!singleRegex.ok()) {
      throw std::runtime_error{absl::StrCat(
          "The regex \"", pattern, "\" ", description,
          " is not a valid regular expression (as understood by Google's RE2 "
          "library): ",
          singleRegex.error())};
    }
  }
  // Each of the patterns is valid on its own, so only their combination could
  // not be compiled (which in practice only happens when RE2 runs out of
  // memory).
  throw std::runtime_error{absl::StrCat(
      "The ", patterns_.size(), " regexes ", description,
      " are each a valid regular expression, but they could not be compiled "
      "into a single automaton (as understood by Google's RE2 library). Try "
      "using fewer or simpler regexes.")};
}

}  // namespace ad_utility
