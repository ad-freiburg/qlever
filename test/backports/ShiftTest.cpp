
#include <gmock/gmock.h>

#include <vector>

#include "backports/shift.h"

using namespace ::testing;

// _____________________________________________________________________________
TEST(Shift, ShiftLeft) {
  std::vector v0{0, 1, 2, 3};
  std::vector v = v0;
  auto it = ql::shift_left(v.begin(), v.end(), 0);
  EXPECT_EQ(it, v.begin());
  EXPECT_THAT(v, ElementsAre(0, 1, 2, 3));

  it = ql::shift_left(v.begin(), v.end(), 4);
  EXPECT_EQ(it, v.begin());
  EXPECT_THAT(v, ElementsAre(0, 1, 2, 3));

  it = ql::shift_left(v.begin(), v.end(), 1);
  EXPECT_EQ(it - v.begin(), 3);
  EXPECT_THAT(v, ElementsAre(1, 2, 3, 3));
  v = v0;
  it = ql::shift_left(v.begin() + 1, v.end(), 1);
  EXPECT_EQ(it - v.begin(), 3);
  EXPECT_THAT(v, ElementsAre(0, 2, 3, 3));
}

// _____________________________________________________________________________
TEST(Shift, ShiftRight) {
  std::vector v0{0, 1, 2, 3};
  std::vector v = v0;
  auto it = ql::shift_right(v.begin(), v.end(), 0);
  EXPECT_EQ(it, v.end());
  EXPECT_THAT(v, ElementsAre(0, 1, 2, 3));

  it = ql::shift_right(v.begin(), v.end(), 4);
  EXPECT_EQ(it, v.end());
  EXPECT_THAT(v, ElementsAre(0, 1, 2, 3));

  it = ql::shift_right(v.begin(), v.end(), 1);
  EXPECT_EQ(it - v.begin(), 1);
  EXPECT_THAT(v, ElementsAre(0, 0, 1, 2));
  v = v0;
  it = ql::shift_right(v.begin() + 1, v.end(), 1);
  EXPECT_EQ(it - v.begin(), 2);
  EXPECT_THAT(v, ElementsAre(0, 1, 1, 2));
}
