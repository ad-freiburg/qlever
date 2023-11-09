//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include <gmock/gmock.h>

#include "util/SourceLocation.h"
#include "util/TypeTraits.h"
#include "util/json.h"

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

// Type that can never be thrown because it can't be built
class NeverThrown {
  NeverThrown() = default;
};

/*
Similar to Gtest's `EXPECT_THROW`. Expect that executing `statement` throws
an exception that inherits from `std::exception`, and that the error message
of that exception, obtained by the `what()` member function, matches the
given `errorMessageMatcher`.

A `errorMessageMatcher` is a google test matcher. More information can be found
here:
https://github.com/google/googletest/blob/main/docs/reference/matchers.md#matchers-reference
*/
#define AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(statement, errorMessageMatcher, \
                                              exceptionType)                  \
  try {                                                                       \
    statement;                                                                \
    FAIL() << "No exception was thrown";                                      \
  } catch (const exceptionType& e) {                                          \
    EXPECT_THAT(e.what(), errorMessageMatcher)                                \
        << "The exception message does not match";                            \
  } catch (const std::conditional_t<                                          \
           ad_utility::isSimilar<exceptionType, std::exception>,              \
           ::NeverThrown, std::exception>& exception) {                       \
    FAIL() << "The thrown exception was "                                     \
           << ::testing::internal::GetTypeName(typeid(exception))             \
           << ", expected " #exceptionType;                                   \
  } catch (...) {                                                             \
    FAIL() << "The thrown exception did not inherit from " #exceptionType;    \
  }                                                                           \
  void()

#define AD_EXPECT_THROW_WITH_MESSAGE(statement, errorMessageMatcher)    \
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(statement, errorMessageMatcher, \
                                        std::exception)
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

// _____________________________________________________________________________

// Helper matcher that allows to use matchers for strings that represent json
// objects.
// Example: EXPECT_THAT("{}", ParsedAsJson(Eq(nlohmann::json::object())));
MATCHER_P(ParsedAsJson, matcher,
          (negation ? "is not json " : "parsed as json ") +
              testing::DescribeMatcher<nlohmann::json>(matcher, negation)) {
  try {
    auto json = nlohmann::json::parse(arg);
    *result_listener << "is a JSON object ";
    return testing::ExplainMatchResult(matcher, json, result_listener);
  } catch (const nlohmann::json::parse_error& error) {
    *result_listener << "is not a JSON object.";
  }
  return false;
}

// Helper matcher that can be used to make assertions about a JSON object's
// values for a certain key. Example:
// EXPECT_THAT((nlohmann::json{{"a", "b"}}), HasKeyMatching("a", Eq("b")));
MATCHER_P2(HasKeyMatching, key, matcher,
           (negation ? "has no key with value " : "has key with value ") +
               testing::DescribeMatcher<nlohmann::json>(matcher, negation)) {
  if (!arg.contains(key)) {
    *result_listener << "that does not contain key \"" << key << '"';
    return false;
  }
  *result_listener << "that contains key \"" << key << "\", with value "
                   << arg[key] << ' ';
  return testing::ExplainMatchResult(matcher, arg[key], result_listener);
}
