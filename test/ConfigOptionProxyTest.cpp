// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (August of 2023, schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <type_traits>

#include "../test/util/ConfigOptionHelpers.h"
#include "../test/util/GTestHelpers.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigOptionProxy.h"

namespace ad_utility {

TEST(ConfigOptionProxy, Constructor) {
  // Test construction for a given type.
  auto doTest = []<typename T>() {
    // Does the normal constructor work?
    T varForConfigOption;
    ConfigOption opt("testOption", "", &varForConfigOption);
    ASSERT_EQ(&opt, &ConfigOptionProxy<T>(opt).getConfigOption());

    // Is there an exception, if we give a config option of the wrong type?
    doForTypeInConfigOptionValueType([&opt]<typename WrongT>() {
      if constexpr (!std::is_same_v<T, WrongT>) {
        AD_EXPECT_THROW_WITH_MESSAGE(
            ConfigOptionProxy<WrongT> someProxy(opt),
            ::testing::ContainsRegex(R"--(testOption': Mismatch)--"));
      }
    });
  };

  // Do the test for all possible types.
  doForTypeInConfigOptionValueType(doTest);
}

}  // namespace ad_utility
