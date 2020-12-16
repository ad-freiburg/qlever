//
// Created by johannes on 07.10.20.
//

#include <gtest/gtest.h>
#include "../../src/engine/QueryExecutionContext.h"
#include "../../src/util/CacheAdapter.h"

auto finish = [](auto&& val) { val.finish(); };
auto nothing = []([[maybe_unused]] auto&& val) {};

using SimpleAdapter =
    ad_utility::CacheAdapter<ad_utility::LRUCache<int, std::string>,
                             decltype(nothing)>;

TEST(CacheAdapter, initialize) { auto a = SubtreeCache(42ul); }

TEST(Adapter, Usage) {
  auto a = SimpleAdapter(nothing, 3ul);
  ASSERT_FALSE(a.getStorage().wlock()->_inProgress.contains(3));
  auto r = a.tryEmplace(3, "");

  ASSERT_FALSE(a.cacheContains(3));
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));
  auto r2 = a.tryEmplace(3, "");
  ASSERT_FALSE(a.cacheContains(3));

  *r._val.first = "hallo";
  r.finish();
  ASSERT_TRUE(a.cacheContains(3));
  ASSERT_EQ("hallo", *a.cacheAt(3));
  ASSERT_EQ("hallo", *r2._val.second);
  ASSERT_EQ("hallo", *r._val.second);
  ASSERT_FALSE(a.getStorage().wlock()->_inProgress.contains(3));
}
