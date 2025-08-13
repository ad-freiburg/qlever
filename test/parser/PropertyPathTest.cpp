//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

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

  auto streamToString = [](const PropertyPath& path) {
    std::stringstream ss;
    ss << path;
    return std::move(ss).str();
  };

  EXPECT_EQ(streamToString(path1), "<http://example.org/path1>");
  EXPECT_EQ(streamToString(path2), "^<http://example.org/path2>");
  EXPECT_EQ(streamToString(path3), "(<http://example.org/path1>){1,3}");
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
  EXPECT_FALSE(path0.getChildOfInvertedPath().has_value());

  auto path1 = PropertyPath::makeInverse(path0);
  EXPECT_EQ(path1.getChildOfInvertedPath(), std::optional{path0});

  auto path2 = PropertyPath::makeNegated(
      {PropertyPath::makeInverse(PropertyPath::fromIri(iri("<a>")))});
  EXPECT_FALSE(path2.getChildOfInvertedPath().has_value());
  auto path3 = PropertyPath::makeAlternative({path1, path2});
  EXPECT_FALSE(path3.getChildOfInvertedPath().has_value());

  auto path4 = PropertyPath::makeSequence({path1, path2});
  EXPECT_FALSE(path4.getChildOfInvertedPath().has_value());

  auto path5 = PropertyPath::makeWithLength(path0, 0, 1);
  EXPECT_FALSE(path5.getChildOfInvertedPath().has_value());
}

// _____________________________________________________________________________
TEST(PropertyPath, handlePath) {
  auto path0 = PropertyPath::fromIri(iri("<a>"));
  EXPECT_EQ(path0.handlePath<int>(
                [](const ad_utility::triple_component::Iri& value) {
                  EXPECT_EQ(value, iri("<a>"));
                  return 0;
                },
                [](const std::vector<PropertyPath>&, PropertyPath::Modifier) {
                  ADD_FAILURE() << "This should not be executed";
                  return 1;
                },
                [](const PropertyPath&, size_t, size_t) {
                  ADD_FAILURE() << "This should not be executed";
                  return 2;
                }),
            0);

  auto path1 = PropertyPath::makeInverse(path0);
  EXPECT_EQ(path1.handlePath<int>(
                [](const ad_utility::triple_component::Iri&) {
                  ADD_FAILURE() << "This should not be executed";
                  return 0;
                },
                [&path0](const std::vector<PropertyPath>& children,
                         PropertyPath::Modifier modifier) {
                  EXPECT_EQ(modifier, PropertyPath::Modifier::INVERSE);
                  EXPECT_THAT(children, ::testing::ElementsAre(path0));
                  return 1;
                },
                [](const PropertyPath&, size_t, size_t) {
                  ADD_FAILURE() << "This should not be executed";
                  return 2;
                }),
            1);

  auto innerPath2 =
      PropertyPath::makeInverse(PropertyPath::fromIri(iri("<a>")));
  auto path2 = PropertyPath::makeNegated({innerPath2});
  EXPECT_EQ(path2.handlePath<int>(
                [](const ad_utility::triple_component::Iri&) {
                  ADD_FAILURE() << "This should not be executed";
                  return 0;
                },
                [&innerPath2](const std::vector<PropertyPath>& children,
                              PropertyPath::Modifier modifier) {
                  EXPECT_EQ(modifier, PropertyPath::Modifier::NEGATED);
                  EXPECT_THAT(children, ::testing::ElementsAre(innerPath2));
                  return 1;
                },
                [](const PropertyPath&, size_t, size_t) {
                  ADD_FAILURE() << "This should not be executed";
                  return 2;
                }),
            1);
  auto path3 = PropertyPath::makeAlternative({path1, path2});
  EXPECT_EQ(path3.handlePath<int>(
                [](const ad_utility::triple_component::Iri&) {
                  ADD_FAILURE() << "This should not be executed";
                  return 0;
                },
                [&path1, &path2](const std::vector<PropertyPath>& children,
                                 PropertyPath::Modifier modifier) {
                  EXPECT_EQ(modifier, PropertyPath::Modifier::ALTERNATIVE);
                  EXPECT_THAT(children, ::testing::ElementsAre(path1, path2));
                  return 1;
                },
                [](const PropertyPath&, size_t, size_t) {
                  ADD_FAILURE() << "This should not be executed";
                  return 2;
                }),
            1);

  auto path4 = PropertyPath::makeSequence({path1, path2});
  EXPECT_EQ(path4.handlePath<int>(
                [](const ad_utility::triple_component::Iri&) {
                  ADD_FAILURE() << "This should not be executed";
                  return 0;
                },
                [&path1, &path2](const std::vector<PropertyPath>& children,
                                 PropertyPath::Modifier modifier) {
                  EXPECT_EQ(modifier, PropertyPath::Modifier::SEQUENCE);
                  EXPECT_THAT(children, ::testing::ElementsAre(path1, path2));
                  return 1;
                },
                [](const PropertyPath&, size_t, size_t) {
                  ADD_FAILURE() << "This should not be executed";
                  return 2;
                }),
            1);

  auto path5 = PropertyPath::makeWithLength(path0, 0, 1);
  EXPECT_EQ(path5.handlePath<int>(
                [](const ad_utility::triple_component::Iri&) {
                  ADD_FAILURE() << "This should not be executed";
                  return 0;
                },
                [](const std::vector<PropertyPath>&, PropertyPath::Modifier) {
                  ADD_FAILURE() << "This should not be executed";
                  return 1;
                },
                [&path0](const PropertyPath& basePath, size_t min, size_t max) {
                  EXPECT_EQ(min, 0);
                  EXPECT_EQ(max, 1);
                  EXPECT_EQ(basePath, path0);
                  return 2;
                }),
            2);
}
