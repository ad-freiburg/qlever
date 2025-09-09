//
// Created by kalmbacj on 9/2/25.
//

#include <gmock/gmock.h>

#include "backports/usingEnum.h"

namespace {

namespace kartoffel {
QL_DEFINE_ENUM(Salat, a, b);

QL_DEFINE_ENUM_MANUAL(Gurke, a, b = 3, c)
QL_ENUM_ALIAS(Gurke, a)
QL_ENUM_ALIAS(Gurke, b)
QL_ENUM_ALIAS(Gurke, c)
QL_DEFINE_ENUM_END();

auto getA() {
  QL_USING_ENUM(Salat);
  return a;
}
auto getB() {
  QL_USING_ENUM(Salat);
  return b;
}

auto getGurkeB() {
  QL_USING_ENUM(Gurke);
  return b;
}
}  // namespace kartoffel

// ________________________________________________________
TEST(UsingEnum, WithAndWithoutNamespace) {
  EXPECT_EQ(static_cast<int>(kartoffel::getA()), 0);
  EXPECT_EQ(static_cast<int>(kartoffel::Salat::a), 0);
  EXPECT_EQ(static_cast<int>(kartoffel::getB()), 1);
  EXPECT_EQ(static_cast<int>(kartoffel::Salat::b), 1);
  EXPECT_EQ(static_cast<int>(kartoffel::getGurkeB()), 3);
  EXPECT_EQ(static_cast<int>(kartoffel::Gurke::b), 3);
  {
    QL_USING_ENUM_NAMESPACE(kartoffel, Salat);
    EXPECT_EQ(static_cast<int>(a), 0);
    EXPECT_EQ(static_cast<int>(b), 1);
  }
  {
    QL_USING_ENUM_NAMESPACE(kartoffel, Gurke);
    EXPECT_EQ(static_cast<int>(a), 0);
    EXPECT_EQ(static_cast<int>(b), 3);
    EXPECT_EQ(static_cast<int>(c), 4);
  }
}

}  // namespace