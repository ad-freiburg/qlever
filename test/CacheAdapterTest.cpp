//
// Created by johannes on 07.10.20.
//

#include <gtest/gtest.h>
#include "../../src/util/CacheAdapter.h"
#include "../../src/engine/QueryExecutionContext.h"


auto finish = [](auto&& val) {val.finish();};

using Adapter = ad_utility::CacheAdapter<SubtreeCache, decltype(finish)>;

TEST(CacheAdapter, initialize) {
  Adapter(finish, 42ul);
}

