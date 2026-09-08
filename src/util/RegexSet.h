// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_REGEXSET_H
#define QLEVER_SRC_UTIL_REGEXSET_H

#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace ad_utility {

// A set of regular expressions that is matched against a string as a whole:
// `matchesAny` returns whether at least one of the regexes matches the complete
// string, just like `RE2::FullMatch` does for a single regex. Internally all
// the regexes are compiled into a single automaton (an `RE2::Set`), so that a
// single pass over the string suffices, no matter how many regexes there are.
//
// A `RegexSet` is cheap to copy (all the state is shared).
class RegexSet {
 private:
  // A `struct` that is only forward-declared here, so that neither `re2/re2.h`
  // nor `re2/set.h` has to be included in this header. It holds all the state
  // of a `RegexSet` (the regexes as strings as well as the compiled
  // automaton), so that copying a `RegexSet` only copies a `shared_ptr`.
  struct Impl;

  // The regexes, `nullptr` if and only if there are none.
  std::shared_ptr<const Impl> impl_;

 public:
  // Default constructor: the empty set of regexes, for which `matchesAny` is
  // always `false`.
  RegexSet() = default;

  // Compile the given `regexesAsStrings`. The `description` says what the
  // regexes are used for (for example ``passed to
  // `--iri-as-blank-node-regexes` ``) and becomes part of the message of the
  // exception that is thrown if one of the `regexesAsStrings` is not a valid
  // regular expression (as understood by Google's RE2 library).
  RegexSet(std::vector<std::string> regexesAsStrings,
           std::string_view description);

  // Return whether at least one of the regexes matches the complete `word`
  // (like `RE2::FullMatch` does for a single regex). Always `false` for an
  // empty set of regexes.
  bool matchesAny(std::string_view word) const;

  // The regexes as strings, in the order in which they were passed to the
  // constructor.
  const std::vector<std::string>& regexesAsStrings() const;

  // The number of regexes, and whether there are none at all.
  size_t size() const { return regexesAsStrings().size(); }
  bool empty() const { return regexesAsStrings().empty(); }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_REGEXSET_H
