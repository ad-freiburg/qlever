// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "../../util/GTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "engine/idTable/IdTableOrSharedIdTableView.h"

using ad_utility::IdTableOrSharedIdTableView;

// _____________________________________________________________________________
TEST(IdTableOrSharedIdTableView, owningTable) {
  auto table = makeIdTableFromVector({{1, 2}, {3, 4}});
  const auto* data = table.getColumn(0).data();
  IdTableOrSharedIdTableView t{std::move(table)};
  EXPECT_TRUE(t.ownsRows());
  EXPECT_EQ(t.view(), makeIdTableFromVector({{1, 2}, {3, 4}}));

  // Moving doesn't invalidate the memory of the rows.
  IdTableOrSharedIdTableView moved{std::move(t)};
  EXPECT_EQ(moved.view().getColumn(0).data(), data);
  IdTable extracted = std::move(moved).extractTable();
  EXPECT_EQ(extracted.getColumn(0).data(), data);
  EXPECT_EQ(extracted, makeIdTableFromVector({{1, 2}, {3, 4}}));
}

// _____________________________________________________________________________
TEST(IdTableOrSharedIdTableView, sharedView) {
  auto owner = std::make_shared<const IdTable>(
      makeIdTableFromVector({{1, 2}, {3, 4}, {5, 6}}));
  std::weak_ptr<const IdTable> weakOwner = owner;
  std::optional<IdTableOrSharedIdTableView> t;
  t.emplace(owner->asStaticView<0>().subView(1, 2), owner);
  owner.reset();
  // The view keeps its owner alive.
  EXPECT_FALSE(weakOwner.expired());
  EXPECT_FALSE(t->ownsRows());
  EXPECT_EQ(t->view(), makeIdTableFromVector({{3, 4}, {5, 6}}));
  AD_EXPECT_THROW_WITH_MESSAGE(std::move(t.value()).extractTable(),
                               ::testing::HasSubstr("ownsRows()"));
  t.reset();
  EXPECT_TRUE(weakOwner.expired());
}
