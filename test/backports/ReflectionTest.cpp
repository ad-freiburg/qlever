// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "backports/reflection.h"

#ifdef QLEVER_HAS_REFLECTION
namespace {
// A class with public and private data members and a static data member.
class Members {
 public:
  int a_;
  static constexpr int notADataMember_ = 0;

 private:
  int b_;
};
struct NoMembers {};

using Names = std::vector<std::string>;

// _____________________________________________________________________________
TEST(Reflection, NamesOfDataMembers) {
  using ql::reflection::namesOfDataMembers;
  static_assert(namesOfDataMembers<Members>() == Names{"a_", "b_"});
  static_assert(namesOfDataMembers<NoMembers>().empty());
}

// _____________________________________________________________________________
TEST(Reflection, JoinNames) {
  using ql::reflection::joinNames;
  static_assert(joinNames({}).empty());
  static_assert(joinNames({"a_"}) == "a_");
  static_assert(joinNames({"a_", "b_", "c_"}) == "a_, b_, c_");
}

// _____________________________________________________________________________
TEST(Reflection, RemoveWhitespace) {
  using ql::reflection::removeWhitespace;
  static_assert(removeWhitespace(" a_,\tb_ ,\n c_\r") == "a_,b_,c_");
  static_assert(removeWhitespace("").empty());
}

// _____________________________________________________________________________
TEST(Reflection, SplitListOfIdentifiers) {
  using ql::reflection::splitListOfIdentifiers;
  static_assert(splitListOfIdentifiers("").empty());
  static_assert(splitListOfIdentifiers(" \n ").empty());
  static_assert(splitListOfIdentifiers("a_") == Names{"a_"});
  static_assert(splitListOfIdentifiers("a_, b_,c_") == Names{"a_", "b_", "c_"});
  static_assert(splitListOfIdentifiers(" a_ ,\n\tb_ ") == Names{"a_", "b_"});
}

// _____________________________________________________________________________
TEST(Reflection, MakeErrorMessage) {
  using ql::reflection::makeErrorMessage;
  using ::testing::AllOf;
  using ::testing::HasSubstr;
  using ::testing::StartsWith;
  // The name of the anonymous namespace in the class name depends on the
  // compiler.
  EXPECT_THAT(makeErrorMessage<Members>("macro", "Something is wrong"),
              AllOf(StartsWith("The macro of `"),
                    HasSubstr("::Members` is invalid: Something is wrong")));
}
}  // namespace
#endif
