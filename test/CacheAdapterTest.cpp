//
// Created by johannes on 07.10.20.
//

#include <gtest/gtest.h>
#include "../../src/util/CacheAdapter.h"
#include "../../src/engine/QueryExecutionContext.h"


auto finish = [](auto&& val) {val.finish();};
auto nothing = []([[maybe_unused]] auto&& val) {};

using Adapter = ad_utility::CacheAdapter<SubtreeCache, decltype(finish)>;

using SimpleAdapter = ad_utility::CacheAdapter<ad_utility::LRUCache<int, std::string>, decltype(nothing)>;

TEST(CacheAdapter, initialize) {
  auto a = Adapter(finish, 42ul);
}

TEST(Adapter, Usage) {
  auto a = SimpleAdapter(nothing, 3ul);
  auto r = a.tryEmplace(3, "");
  ASSERT_FALSE(a.getStorage().wlock()->_cache.contains(3));
  *r._val.first = "hallo";
  r.finish();
  ASSERT_TRUE(a.getStorage().wlock()->_cache.contains(3));
  ASSERT_EQ("hallo", *a.getStorage().wlock()->_cache.operator[](3));


}


