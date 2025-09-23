// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/GTestHelpers.h"
#include "util/TimeTracer.h"

using json = nlohmann::ordered_json;
using namespace testing;

// Example tracer used in the tests below.
auto makeTracer = []() {
  ad_utility::timer::TimeTracer tracer("test");
  tracer.beginTrace("a");
  tracer.beginTrace("b");
  tracer.endTrace("b");
  tracer.endTrace("a");
  tracer.beginTrace("c");
  tracer.endTrace("c");
  tracer.endTrace("test");
  return tracer;
};

// Check the structure of the "short" JSON is as expected.
TEST(TimeTracerTest, to_json_short) {
  {
    auto tracer = makeTracer();
    EXPECT_THAT(
        tracer.getJSONShort(),
        HasKeyMatching(
            "test",
            testing::AllOf(HasKey("total"),
                           HasKeyMatching("a", testing::AllOf(HasKey("total"),
                                                              HasKey("b"))),
                           HasKey("c"))));
  }
  {
    ad_utility::timer::DefaultTimeTracer tracer("default");
    EXPECT_THAT(tracer.getJSONShort(), testing::IsEmpty());
  }
}

// Check the structure of the "long" JSON is as expected.
TEST(TimeTracerTest, to_json) {
  {
    auto tracer = makeTracer();
    EXPECT_THAT(
        tracer.getJSON(),
        testing::AllOf(
            HasKeyMatching("name", testing::Eq("test")), HasKey("begin"),
            HasKey("end"), HasKey("duration"),
            HasKeyMatching(
                "children",
                testing::ElementsAre(
                    testing::AllOf(
                        HasKeyMatching("name", testing::Eq("a")),
                        HasKey("begin"), HasKey("end"), HasKey("duration"),
                        HasKeyMatching(
                            "children",
                            testing::ElementsAre(testing::AllOf(
                                HasKeyMatching("name", testing::Eq("b")),
                                HasKey("begin"), HasKey("end"),
                                HasKey("duration"))))),
                    testing::AllOf(HasKeyMatching("name", testing::Eq("c")),
                                   HasKey("begin"), HasKey("end"),
                                   HasKey("duration"))))));
  }
  {
    ad_utility::timer::DefaultTimeTracer tracer("default");
    EXPECT_THAT(tracer.getJSON(), testing::IsEmpty());
  }
}

// Check that exceptions are thrown when the tracer is misused.
TEST(TimeTracerTest, exceptions) {
  ad_utility::timer::TimeTracer tracer("test");
  AD_EXPECT_THROW_WITH_MESSAGE(
      tracer.reset(),
      testing::HasSubstr("Cannot reset a TimeTracer that has active traces."));
  tracer.endTrace("test");
  AD_EXPECT_THROW_WITH_MESSAGE(tracer.endTrace("test"),
                               testing::HasSubstr("The trace has ended."));
  AD_EXPECT_THROW_WITH_MESSAGE(tracer.beginTrace("test"),
                               testing::HasSubstr("The trace has ended."));
}
