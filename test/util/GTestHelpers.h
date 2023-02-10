//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include "gmock/gmock.h"
#include "util/SourceLocation.h"

// The following two macros make the usage of `testing::Property` and
// `testing::Field` simpler and more consistent. Examples:
//  AD_PROPERTY(std::string, empty, IsTrue);  // Matcher that checks that
//  `arg.empty()` is true for the passed std::string `arg`.
// AD_FIELD(std::pair<int, bool>, second, IsTrue); // Matcher that checks, that
// `arg.second` is true for a`std::pair<int, bool>`

#ifdef AD_PROPERTY
#error "AD_PROPERTY must not already be defined. Consider renaming it."
#else
#define AD_PROPERTY(Class, Member, Matcher) \
  ::testing::Property(#Member "()", &Class::Member, Matcher)
#endif

#ifdef AD_FIELD
#error "AD_FIELD must not already be defined. Consider renaming it."
#else
#define AD_FIELD(Class, Member, Matcher) \
  ::testing::Field(#Member, &Class::Member, Matcher)
#endif

// _____________________________________________________________________________
// Add the given `source_location`  to all gtest failure messages that occur,
// while the return value is still in scope. It is important to bind the return
// value to a variable, otherwise it will immediately go of scope and have no
// effect.
[[nodiscard]] inline testing::ScopedTrace generateLocationTrace(
    ad_utility::source_location l,
    std::string_view errorMessage = "Actual location of the test failure") {
  return {l.file_name(), static_cast<int>(l.line()), errorMessage};
}
