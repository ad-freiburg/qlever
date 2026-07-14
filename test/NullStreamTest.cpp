// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>

#include "util/NullStream.h"

using ad_utility::NullStream;

// Writing arbitrary data to a `NullStream` discards it and leaves the stream in
// a valid, good state.
TEST(NullStream, discardsInputAndStaysGood) {
  NullStream stream;
  EXPECT_TRUE(stream.good());

  stream << "some text" << 42 << ' ' << 3.14 << std::string(1000, 'x');

  EXPECT_TRUE(stream.good());
  EXPECT_FALSE(stream.fail());
  EXPECT_FALSE(stream.bad());
}

// A `NullStream` can be used through a `std::ostream&` reference like any other
// output stream.
TEST(NullStream, usableAsOstreamReference) {
  NullStream stream;
  std::ostream& out = stream;
  out << "written via base-class reference";
  EXPECT_TRUE(out.good());
}
