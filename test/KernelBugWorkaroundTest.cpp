// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>
#include <sys/types.h>

#include "util/KernelBugWorkaround.h"

#ifdef __linux__
// The control interface of `jemalloc` (a null pointer without `jemalloc`).
extern "C" int mallctl(const char* name, void* oldp, size_t* oldlenp,
                       void* newp, size_t newlen) __attribute__((weak));
#endif

using namespace ad_utility::kernelBugWorkaround;

// _____________________________________________________________________________
TEST(KernelBugWorkaround, mayHaveBug) {
  // Affected arm64 kernels.
  EXPECT_TRUE(mayHaveBug("aarch64", "7.0.0-1017-azure"));
  EXPECT_TRUE(mayHaveBug("aarch64", "7.0.0-rc1"));
  EXPECT_TRUE(mayHaveBug("aarch64", "7.0.14"));
  EXPECT_TRUE(mayHaveBug("arm64", "7.0"));
  EXPECT_TRUE(mayHaveBug("aarch64", "7.1"));
  EXPECT_TRUE(mayHaveBug("aarch64", "7.1.8-200.fc44.aarch64"));
  EXPECT_TRUE(mayHaveBug("aarch64", "7.2.0-rc6"));

  // Fixed or older arm64 kernels.
  EXPECT_FALSE(mayHaveBug("aarch64", "7.1.9"));
  EXPECT_FALSE(mayHaveBug("aarch64", "7.1.12-generic"));
  EXPECT_FALSE(mayHaveBug("aarch64", "7.2.0-rc7"));
  EXPECT_FALSE(mayHaveBug("aarch64", "7.2.0"));
  EXPECT_FALSE(mayHaveBug("aarch64", "7.3.0-10-generic"));
  EXPECT_FALSE(mayHaveBug("aarch64", "6.17.0-1022-azure"));
  EXPECT_FALSE(mayHaveBug("aarch64", "6.8.0-1070-azure"));
  EXPECT_FALSE(mayHaveBug("aarch64", "17.0.0"));
  EXPECT_FALSE(mayHaveBug("aarch64", "8.0.0"));

  // Other architectures.
  EXPECT_FALSE(mayHaveBug("x86_64", "7.0.0-38-generic"));
  EXPECT_FALSE(mayHaveBug("riscv64", "7.1.0"));

  // Releases that can't be parsed.
  EXPECT_FALSE(mayHaveBug("aarch64", ""));
  EXPECT_FALSE(mayHaveBug("aarch64", "7"));
  EXPECT_FALSE(mayHaveBug("aarch64", "custom-kernel"));
}

// _____________________________________________________________________________
TEST(KernelBugWorkaround, applyIfNeeded) {
  EXPECT_EQ(applyIfNeeded("x86_64", "7.0.0-38-generic", false),
            Status::NotNeeded);
  EXPECT_EQ(applyIfNeeded("aarch64", "7.2.0", false), Status::NotNeeded);
  EXPECT_EQ(applyIfNeeded("aarch64", "7.0.0-1017-azure", true),
            Status::DisabledByUser);
  // The overload without arguments must work on any machine.
  [[maybe_unused]] auto status = applyIfNeeded();

#ifndef __linux__
  // The workaround is only available on Linux.
  EXPECT_EQ(applyIfNeeded(), Status::NotNeeded);
  EXPECT_EQ(applyIfNeeded("aarch64", "7.0.0-1017-azure", false),
            Status::NoJemalloc);
#else
  if (mallctl == nullptr) {
    EXPECT_EQ(applyIfNeeded("aarch64", "7.0.0-1017-azure", false),
              Status::NoJemalloc);
    return;
  }
  EXPECT_EQ(applyIfNeeded("aarch64", "7.0.0-1017-azure", false),
            Status::Applied);
  // The default for new arenas and the setting of arena 0 are now -1.
  ssize_t decay = 0;
  size_t size = sizeof(decay);
  ASSERT_EQ(mallctl("arenas.dirty_decay_ms", &decay, &size, nullptr, 0), 0);
  EXPECT_EQ(decay, -1);
  decay = 0;
  ASSERT_EQ(mallctl("arena.0.dirty_decay_ms", &decay, &size, nullptr, 0), 0);
  EXPECT_EQ(decay, -1);
#endif
}
