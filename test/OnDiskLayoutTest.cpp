// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdint>

#include "util/OnDiskLayout.h"

// The types for the tests are defined in a named namespace, such that the tests
// also cover that the `QL_ASSERT_ON_DISK_LAYOUT` of a class is found for the
// members of other classes (via argument-dependent lookup).
namespace onDiskLayoutTest {
enum struct Color : uint8_t { Red, Green };

// A class with an asserted layout, which other classes refer to by its name.
struct Inner {
  uint64_t x_;
  uint64_t y_;
};
QL_ASSERT_ON_DISK_LAYOUT(Inner, "x_:u64, y_:u64", "Update the version");

// A class without an asserted layout, which is expanded inside other classes.
struct Plain {
  uint32_t a_;
  float b_;
};

// A class that has a member of each of the supported kinds, some of them
// private.
class AllKinds {
  Inner inner_;
  Plain plain_;
  std::array<char, 4> tag_;
  Color color_;
  bool flag_;
  int16_t array_[1];
  uint64_t manyBits_ : 60;
  uint64_t fewBits_ : 4;
  struct Nested {
    double d_;
  } nested_;
  const int64_t constant_ = 0;

 public:
  struct NestedClass {
    uint8_t value_;
  };
};
// Note that whitespace (including the line breaks between the adjacent string
// literals) is ignored.
QL_ASSERT_ON_DISK_LAYOUT(
    AllKinds,
    "inner_:Inner, plain_:{a_:u32,b_:f32},  tag_:[4]char, color_:enum<u8>, "
    "flag_:bool,array_:[1]i16, manyBits_:u64:60, fewBits_:u64:4, "
    "nested_:{d_:f64}, constant_:i64",
    "Update the version");
QL_ASSERT_ON_DISK_LAYOUT(AllKinds::NestedClass, "value_:u8",
                         "Update the version");

// A class whose members are of the nested class with an asserted layout.
struct Outer {
  std::array<AllKinds::NestedClass, 8> values_;
};
QL_ASSERT_ON_DISK_LAYOUT(Outer, "values_:[8]NestedClass", "Update the version");

// Classes that are not suitable for being written to disk.
struct PaddingInside {
  uint8_t a_;
  uint64_t b_;
};
struct PaddingAtEnd {
  uint64_t a_;
  uint8_t b_;
};
struct PaddingInMember {
  uint64_t a_;
  PaddingAtEnd b_;
};
struct WithPointer {
  int* pointer_;
};
struct WithUnion {
  union {
    uint64_t a_;
    double b_;
  } union_;
};
struct Derived : Inner {
  uint64_t z_;
};
struct NotTriviallyCopyable {
  uint64_t a_;
  NotTriviallyCopyable(const NotTriviallyCopyable&) {}
};
}  // namespace onDiskLayoutTest

#ifdef QLEVER_HAS_REFLECTION
namespace {
using namespace onDiskLayoutTest;
using ad_utility::onDiskLayout::checkLayout;
using ::testing::AllOf;
using ::testing::HasSubstr;
constexpr std::string_view hint = "Update the version";

// Test the error messages for a layout that differs from the asserted one.
TEST(OnDiskLayout, WrongLayout) {
  static_assert(checkLayout<Inner>("x_:u64, y_:u64", hint).empty());
  static_assert(checkLayout<Inner>(" x_ : u64,\ny_:u64 ", hint).empty());
  auto expectMismatch = [](std::string_view message,
                           std::string_view expected) {
    EXPECT_THAT(
        message,
        AllOf(HasSubstr("`onDiskLayoutTest::Inner`"),
              HasSubstr("The layout of the class is `x_:u64, y_:u64`, "
                        "but the asserted layout is `"),
              HasSubstr(expected), HasSubstr("`. Update the version.")));
  };
  // A wrong type, a wrong name, a wrong order, a missing member, and an
  // additional member.
  expectMismatch(checkLayout<Inner>("x_:u64, y_:i64", hint), "x_:u64, y_:i64");
  expectMismatch(checkLayout<Inner>("x_:u64, z_:u64", hint), "x_:u64, z_:u64");
  expectMismatch(checkLayout<Inner>("y_:u64, x_:u64", hint), "y_:u64, x_:u64");
  expectMismatch(checkLayout<Inner>("x_:u64", hint), "x_:u64");
  expectMismatch(checkLayout<Inner>("x_:u64, y_:u64, z_:u64", hint),
                 "x_:u64, y_:u64, z_:u64");
}

// Test the error messages for classes that cannot be written to disk.
TEST(OnDiskLayout, UnsupportedClasses) {
  EXPECT_THAT(checkLayout<PaddingInside>("", hint),
              AllOf(HasSubstr("`onDiskLayoutTest::PaddingInside`"),
                    HasSubstr("56 bits of implicit padding before the member "
                              "`b_`")));
  EXPECT_THAT(checkLayout<PaddingAtEnd>("", hint),
              HasSubstr("56 bits of implicit padding at its end"));
  EXPECT_THAT(checkLayout<PaddingInMember>("", hint),
              AllOf(HasSubstr("`onDiskLayoutTest::PaddingAtEnd` has 56 bits of "
                              "implicit padding at its end")));
  EXPECT_THAT(checkLayout<WithPointer>("", hint),
              HasSubstr("The type `int*` cannot be written to disk"));
  EXPECT_THAT(checkLayout<WithUnion>("", hint),
              HasSubstr("is a union, which is not supported"));
  EXPECT_THAT(checkLayout<Derived>("", hint),
              HasSubstr("has base classes, which is not supported"));
  EXPECT_THAT(checkLayout<NotTriviallyCopyable>("", hint),
              HasSubstr("The class is not trivially copyable"));
}
}  // namespace
#else
// Without reflection, the layouts above are not checked, so there is nothing to
// test at runtime. This test only exists, such that the test binary is not
// empty.
TEST(OnDiskLayout, CompilesWithoutReflection) {
  QL_ASSERT_ON_DISK_LAYOUT(onDiskLayoutTest::PaddingInside,
                           "The layout is not checked without reflection",
                           "Update the version");
}
#endif
