//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "parser/PropertyPath.h"

using ad_utility::triple_component::Iri;

namespace {
auto iri1 = Iri::fromIriref("<http://example.org/path1>");
auto iri2 = Iri::fromIriref("<http://example.org/path2>");

auto iri = [](std::string_view iriStr) { return Iri::fromIriref(iriStr); };
}  // namespace

TEST(PropertyPath, BasicPathEquality) {
  auto path1 = PropertyPath::fromIri(iri1);
  auto path2 = PropertyPath::fromIri(iri1);
  auto path3 = PropertyPath::fromIri(iri2);

  EXPECT_EQ(path1, path2);
  EXPECT_NE(path1, path3);
}

// _____________________________________________________________________________
TEST(PropertyPath, ModifiedPathEquality) {
  auto path1 = PropertyPath::makeInverse(PropertyPath::fromIri(iri1));
  auto path2 = PropertyPath::makeInverse(PropertyPath::fromIri(iri1));
  auto path3 = PropertyPath::makeInverse(PropertyPath::fromIri(iri2));

  EXPECT_EQ(path1, path2);
  EXPECT_NE(path1, path3);

  auto path4 = PropertyPath::makeAlternative(
      {PropertyPath::fromIri(iri1), PropertyPath::fromIri(iri2)});
  auto path5 = PropertyPath::makeAlternative(
      {PropertyPath::fromIri(iri2), PropertyPath::fromIri(iri1)});
  EXPECT_NE(path4, path5);

  auto path6 = PropertyPath::makeSequence(
      {PropertyPath::fromIri(iri1), PropertyPath::fromIri(iri2)});
  auto path7 = PropertyPath::makeSequence(
      {PropertyPath::fromIri(iri2), PropertyPath::fromIri(iri1)});
  EXPECT_NE(path6, path7);
  EXPECT_NE(path4, path6);
  EXPECT_NE(path5, path7);

  auto path8 = PropertyPath::makeNegated(
      {PropertyPath::fromIri(iri1), PropertyPath::fromIri(iri2)});
  auto path9 = PropertyPath::makeNegated(
      {PropertyPath::fromIri(iri2), PropertyPath::fromIri(iri1)});

  EXPECT_NE(path8, path9);
  EXPECT_NE(path4, path8);
  EXPECT_NE(path5, path9);
  EXPECT_NE(path6, path8);
  EXPECT_NE(path7, path9);

  auto path10 = PropertyPath::makeAlternative(
      {PropertyPath::fromIri(iri1), PropertyPath::fromIri(iri2)});

  EXPECT_EQ(path10, path4);

  auto path11 = PropertyPath::makeSequence(
      {PropertyPath::fromIri(iri1), PropertyPath::fromIri(iri2)});

  EXPECT_EQ(path11, path6);

  auto path12 = PropertyPath::makeNegated(
      {PropertyPath::fromIri(iri1), PropertyPath::fromIri(iri2)});

  EXPECT_EQ(path12, path8);
}

// _____________________________________________________________________________
TEST(PropertyPath, MinMaxPathEquality) {
  auto path1 = PropertyPath::makeWithLength(PropertyPath::fromIri(iri1), 1, 3);
  auto path2 = PropertyPath::makeWithLength(PropertyPath::fromIri(iri1), 1, 3);
  auto path3 = PropertyPath::makeWithLength(PropertyPath::fromIri(iri2), 1, 3);
  auto path4 = PropertyPath::makeWithLength(PropertyPath::fromIri(iri1), 2, 3);
  auto path5 = PropertyPath::makeWithLength(PropertyPath::fromIri(iri1), 2, 2);

  EXPECT_EQ(path1, path2);
  EXPECT_NE(path1, path3);
  EXPECT_NE(path1, path4);
  EXPECT_NE(path1, path5);
  EXPECT_NE(path3, path4);
  EXPECT_NE(path3, path5);
  EXPECT_NE(path4, path5);
}

// _____________________________________________________________________________
TEST(PropertyPath, MinMaxPathCopyAssignment) {
  // Test that the copy assignment operator works correctly for MinMaxPath.
  auto path1 = PropertyPath::makeWithLength(PropertyPath::fromIri(iri1), 1, 3);
  auto path2 = path1;
  auto path3 = PropertyPath::makeWithLength(PropertyPath::fromIri(iri2), 2, 4);

  path1 = path3;

  EXPECT_NE(path1, path2);
  EXPECT_EQ(path1, path3);
}

// _____________________________________________________________________________
TEST(PropertyPath, MinMaxPathMoveAssignment) {
  // Test that the move assignment operator works correctly for MinMaxPath.
  auto path1 = PropertyPath::makeWithLength(PropertyPath::fromIri(iri1), 1, 3);
  auto path2 = std::move(path1);
  auto path3 = PropertyPath::makeWithLength(PropertyPath::fromIri(iri2), 2, 4);

  path1 = std::move(path3);

  EXPECT_NE(path1, path2);
}

// _____________________________________________________________________________
TEST(PropertyPath, OstreamOutput) {
  auto path1 = PropertyPath::fromIri(iri1);
  auto path2 = PropertyPath::makeInverse(PropertyPath::fromIri(iri2));
  auto path3 = PropertyPath::makeWithLength(PropertyPath::fromIri(iri1), 1, 3);

  std::stringstream ss;
  ss << path1;
  EXPECT_EQ(ss.str(), "<http://example.org/path1>");

  ss.str("");
  ss << path2;
  EXPECT_EQ(ss.str(), "^<http://example.org/path2>");

  ss.str("");
  ss << path3;
  EXPECT_EQ(ss.str(), "(<http://example.org/path1>){1,3}");
}

// _____________________________________________________________________________
TEST(PropertyPath, propertyPathsFormatting) {
  {
    auto path = PropertyPath::makeNegated(
        {PropertyPath::makeInverse(PropertyPath::fromIri(iri("<a>")))});
    EXPECT_EQ("!((^<a>))", path.asString());
  }
  {
    auto path = PropertyPath::makeNegated(
        {PropertyPath::makeInverse(PropertyPath::fromIri(iri("<a>"))),
         PropertyPath::fromIri(iri("<b>"))});
    EXPECT_EQ("!((^<a>)|<b>)", path.asString());
  }
  {
    auto path = PropertyPath::makeNegated({});
    EXPECT_EQ("!()", path.asString());
  }
  {
    auto path = PropertyPath::makeSequence({PropertyPath::fromIri(iri("<a>")),
                                            PropertyPath::fromIri(iri("<a>")),
                                            PropertyPath::fromIri(iri("<b>"))});
    EXPECT_EQ("<a>/<a>/<b>", path.asString());
  }
}

// _____________________________________________________________________________
TEST(PropertyPath, getInvertedChild) {
  auto path0 = PropertyPath::fromIri(iri("<a>"));
  EXPECT_FALSE(path0.getInvertedChild().has_value());

  auto path1 = PropertyPath::makeInverse(path0);
  EXPECT_EQ(path1.getInvertedChild(), std::optional{path0});

  auto path2 = PropertyPath::makeNegated(
      {PropertyPath::makeInverse(PropertyPath::fromIri(iri("<a>")))});
  EXPECT_FALSE(path2.getInvertedChild().has_value());
  auto path3 = PropertyPath::makeAlternative({path1, path2});
  EXPECT_FALSE(path3.getInvertedChild().has_value());

  auto path4 = PropertyPath::makeSequence({path1, path2});
  EXPECT_FALSE(path4.getInvertedChild().has_value());

  auto path5 = PropertyPath::makeWithLength(path0, 0, 1);
  EXPECT_FALSE(path5.getInvertedChild().has_value());
}
