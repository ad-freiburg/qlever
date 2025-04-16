//
// Created by kalmbacj on 2/6/25.
//

#include <gmock/gmock.h>

#include "../util/IdTableHelpers.h"
#include "engine/NamedQueryCache.h"

TEST(NamedQueryCache, basicWorkflow) {
  NamedQueryCache cache;
  EXPECT_EQ(cache.numEntries(), 0);
  auto table = makeIdTableFromVector({{3, 7}, {9, 11}});
  using V = Variable;
  VariableToColumnMap varColMap{{V{"?x"}, makeAlwaysDefinedColumn(0)},
                                {V{"?y"}, makeAlwaysDefinedColumn(1)}};
  NamedQueryCache::Value value{
      std::make_shared<const IdTable>(table.clone()), varColMap, {1, 0}};
  cache.store("query-1", std::move(value));
  EXPECT_EQ(cache.numEntries(), 1);
}
