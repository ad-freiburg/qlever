// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "engine/HttpError.h"

TEST(HttpErrorTest, fields) {
  {
    // The reason is set automatically based on the status code.
    HttpError err(boost::beast::http::status::not_found);
    EXPECT_THAT(err.status(),
                testing::Eq(boost::beast::http::status::not_found));
    EXPECT_THAT(err.what(), testing::StrEq("Not Found"));
  }
  {
    // With an explicitly set reason.
    HttpError err(boost::beast::http::status::not_found, "Sorry");
    EXPECT_THAT(err.status(),
                testing::Eq(boost::beast::http::status::not_found));
    EXPECT_THAT(err.what(), testing::StrEq("Sorry"));
  }
}
