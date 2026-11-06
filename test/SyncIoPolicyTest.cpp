// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "util/GTestHelpers.h"
#include "util/IoUringManager.h"
#include "util/IoUringManagerTestHelpers.h"

namespace {

using namespace ::testing;
using namespace ioUringManagerTestHelpers;

// A read that is fully satisfied returns the requested bytes from the requested
// offset.
TEST(ReadFullyOrThrow, FullReadSucceeds) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");
  std::vector<char> targetBuffer(4);
  ad_utility::SyncIoPolicy::readFullyOrThrow(fd, targetBuffer.data(), 4, 4);
  EXPECT_EQ(std::string(targetBuffer.data(), 4), "BBBB");
}

// An invalid file descriptor makes the underlying `pread` call fail (return
// -1), which must cause a throw.
TEST(ReadFullyOrThrow, PreadFailureThrows) {
  std::vector<char> targetBuffer(4);
  constexpr int invalidFd = -1;
  AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::SyncIoPolicy::readFullyOrThrow(
                                   invalidFd, targetBuffer.data(), 4, 0),
                               HasSubstr("pread failed"));
}

// Requesting more bytes than the file contains (read past EOF) is a short read
// and must throw.
TEST(ReadFullyOrThrow, ShortReadThrows) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");  // 8 bytes
  std::vector<char> targetBuffer(16);
  AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::SyncIoPolicy::readFullyOrThrow(
                                   fd, targetBuffer.data(), 16, 0),
                               HasSubstr("read fewer bytes than requested"));
}

}  // namespace
