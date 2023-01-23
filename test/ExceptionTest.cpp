//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/Algorithm.h"
#include "util/Exception.h"

using namespace ad_utility;

void checkContains(const std::exception& e, std::string_view needle) {
  ASSERT_TRUE(ad_utility::contains(std::string_view{e.what()}, needle));
}

TEST(Exception, AbortException) {
  AbortException a{std::runtime_error{"errorA"}};
  ASSERT_STREQ(a.what(), "errorA");
  AbortException b{"errorB"};
  ASSERT_STREQ(b.what(), "errorB");
}

TEST(Exception, Exception) {
  ad_utility::source_location l = ad_utility::source_location::current();
  Exception e{"exceptionE"};
  ASSERT_THAT(e.what(), ::testing::StartsWith("exceptionE"));
  ASSERT_THAT(e.what(), ::testing::EndsWith(std::to_string(l.line() + 1)));
  checkContains(e, l.file_name());
}

TEST(Exception, AD_THROW) {
  ad_utility::source_location l;
  try {
    l = ad_utility::source_location::current();
    AD_THROW("anError");
    FAIL() << "No exception was thrown, but one was expected";
  } catch (const Exception& e) {
    ASSERT_THAT(e.what(), ::testing::StartsWith("anError"));
    ASSERT_THAT(e.what(), ::testing::EndsWith(std::to_string(l.line() + 1)));
    checkContains(e, l.file_name());
  }
}

TEST(Exception, AD_CHECK) {
  ad_utility::source_location l;
  ASSERT_NO_THROW(AD_CHECK(3 < 5));
  std::vector<int> v;
  ASSERT_NO_THROW(AD_CHECK(v.empty()));
  try {
    v.push_back(27);
    l = ad_utility::source_location::current();
    AD_CHECK(v.empty());
    FAIL() << "No exception was thrown, but one was expected";
  } catch (const Exception& e) {
    ASSERT_THAT(e.what(),
                ::testing::StartsWith("Assertion `v.empty()` failed. In file"));
    ASSERT_THAT(e.what(), ::testing::EndsWith(std::to_string(l.line() + 1)));
    checkContains(e, l.file_name());
  }
}

TEST(Exception, AD_UNSATISFIABLE) {
  ad_utility::source_location l;
  ASSERT_NO_THROW(AD_UNSATISFIABLE(3 < 5));
  std::vector<int> v;
  ASSERT_NO_THROW(AD_UNSATISFIABLE(v.empty()));
  try {
    v.push_back(27);
    l = ad_utility::source_location::current();
    AD_UNSATISFIABLE(v.empty());
    FAIL() << "No exception was thrown, but one was expected";
  } catch (const Exception& e) {
    ASSERT_THAT(e.what(),
                ::testing::StartsWith("Assertion `v.empty()` failed. In file"));
    ASSERT_THAT(e.what(), ::testing::EndsWith(std::to_string(l.line() + 1)));
    checkContains(e, l.file_name());
  }
}
