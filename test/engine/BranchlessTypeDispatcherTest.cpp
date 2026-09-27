// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/BranchlessTypeDispatcher.h"
#include "global/Id.h"
#include "global/ValueId.h"

using namespace ql::engine;

TEST(BranchlessTypeDispatcherTest, FormatIri) {
  std::array<char, 256> buffer{};
  auto id = ValueId::makeFromVocabIndex(VocabIndex::make(42));
  std::string_view rawTerm = "http://example.org/resource";

  char* end = BranchlessTypeDispatcher::dispatchTermFormat(
      id, rawTerm, buffer.data(), BranchlessTypeDispatcher::defaultLut());
  std::string_view result(buffer.data(), end - buffer.data());
  EXPECT_EQ(result, "<http://example.org/resource>");
}

TEST(BranchlessTypeDispatcherTest, FormatLiteral) {
  std::array<char, 256> buffer{};
  auto id = ValueId::makeFromTextRecordIndex(TextRecordIndex::make(10));
  std::string_view rawTerm = "Hello World";

  char* end = BranchlessTypeDispatcher::dispatchTermFormat(
      id, rawTerm, buffer.data(), BranchlessTypeDispatcher::defaultLut());
  std::string_view result(buffer.data(), end - buffer.data());
  EXPECT_EQ(result, "\"Hello World\"");
}

TEST(BranchlessTypeDispatcherTest, FormatInteger) {
  std::array<char, 256> buffer{};
  auto id = ValueId::makeFromInt(123456);

  char* end = BranchlessTypeDispatcher::dispatchTermFormat(
      id, "", buffer.data(), BranchlessTypeDispatcher::defaultLut());
  std::string_view result(buffer.data(), end - buffer.data());
  EXPECT_EQ(result, "\"123456\"^^<http://www.w3.org/2001/XMLSchema#integer>");
}

TEST(BranchlessTypeDispatcherTest, FormatDouble) {
  std::array<char, 256> buffer{};
  // 3.5 is exactly representable even in `ValueId`'s 49-bit truncated double
  // mantissa; values like 3.14159 do not round-trip through `makeFromDouble`
  // and must not be used for exact string expectations here.
  auto id = ValueId::makeFromDouble(3.5);

  char* end = BranchlessTypeDispatcher::dispatchTermFormat(
      id, "", buffer.data(), BranchlessTypeDispatcher::defaultLut());
  std::string_view result(buffer.data(), end - buffer.data());
  EXPECT_TRUE(ql::starts_with(result, "\"3.5\""));
  EXPECT_TRUE(
      ql::ends_with(result, "\"^^<http://www.w3.org/2001/XMLSchema#double>"));
}

TEST(BranchlessTypeDispatcherTest, FormatBoolean) {
  std::array<char, 256> buffer{};
  auto idTrue = ValueId::makeFromBool(true);
  auto idFalse = ValueId::makeFromBool(false);

  char* end1 = BranchlessTypeDispatcher::dispatchTermFormat(
      idTrue, "", buffer.data(), BranchlessTypeDispatcher::defaultLut());
  EXPECT_EQ(std::string_view(buffer.data(), end1 - buffer.data()),
            "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>");

  char* end2 = BranchlessTypeDispatcher::dispatchTermFormat(
      idFalse, "", buffer.data(), BranchlessTypeDispatcher::defaultLut());
  EXPECT_EQ(std::string_view(buffer.data(), end2 - buffer.data()),
            "\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>");
}

TEST(BranchlessTypeDispatcherTest, FormatBlankNode) {
  std::array<char, 256> buffer{};
  auto id = ValueId::makeFromBlankNodeIndex(BlankNodeIndex::make(9876));

  char* end = BranchlessTypeDispatcher::dispatchTermFormat(
      id, "", buffer.data(), BranchlessTypeDispatcher::defaultLut());
  std::string_view result(buffer.data(), end - buffer.data());
  EXPECT_EQ(result, "_:bn9876");
}

TEST(BranchlessTypeDispatcherTest, FormatSecondaryVocabIndex) {
  std::array<char, 256> buffer{};
  auto id = ValueId::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(7));
  std::string_view rawTerm = "http://example.org/secondary";

  char* end = BranchlessTypeDispatcher::dispatchTermFormat(
      id, rawTerm, buffer.data(), BranchlessTypeDispatcher::defaultLut());
  std::string_view result(buffer.data(), end - buffer.data());
  EXPECT_EQ(result, "<http://example.org/secondary>");
}

TEST(BranchlessTypeDispatcherTest, FormatTurtleCompact) {
  std::array<char, 256> buffer{};
  auto idInt = ValueId::makeFromInt(42);

  char* end = BranchlessTypeDispatcher::dispatchTermFormat(
      idInt, "", buffer.data(), BranchlessTypeDispatcher::turtleLut());
  std::string_view result(buffer.data(), end - buffer.data());
  EXPECT_EQ(result, "42");
}

TEST(BranchlessTypeDispatcherTest, BatchFormatting) {
  std::vector<ValueId> ids = {
      ValueId::makeFromVocabIndex(VocabIndex::make(1)),
      ValueId::makeFromInt(100),
      ValueId::makeFromBlankNodeIndex(BlankNodeIndex::make(5)),
      ValueId::makeFromBool(true)};
  std::vector<std::string_view> rawTerms = {"http://example.org/pred", "", "",
                                            ""};
  std::array<char, 1024> buffer{};

  size_t bytesWritten = BranchlessTypeDispatcher::dispatchBatchTermFormat(
      ids, rawTerms, buffer.data(), BranchlessTypeDispatcher::turtleLut());

  std::string_view output(buffer.data(), bytesWritten);
  EXPECT_EQ(output, "<http://example.org/pred>100_:bn5true");
}
