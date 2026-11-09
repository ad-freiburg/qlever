// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sys/types.h>

#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "backports/three_way_comparison.h"
#include "util/KernelBugWorkaround.h"

#ifdef __linux__
// The control interface of `jemalloc` (a null pointer without `jemalloc`).
extern "C" int mallctl(const char* name, void* oldp, size_t* oldlenp,
                       void* newp, size_t newlen) __attribute__((weak));
#endif

using namespace ad_utility::kernelBugWorkaround;

namespace {
// A call to the mock of `mallctl`: the name of the setting and the value that
// was written (if any).
struct MallctlCall {
  std::string name_;
  std::optional<ssize_t> writtenValue_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(MallctlCall, name_, writtenValue_)
};

// Return a mock of `mallctl` that appends each call to `calls`. The mock fails
// for the setting `failingName`, reports `numArenas` for `arenas.narenas`, and
// succeeds for all other settings.
MallctlFunction makeMallctlMock(std::vector<MallctlCall>& calls,
                                std::string failingName = "",
                                unsigned numArenas = 0) {
  return [&calls, failingName = std::move(failingName), numArenas](
             const char* name, void* oldp, size_t* oldlenp, void* newp,
             size_t newlen) -> int {
    std::optional<ssize_t> writtenValue;
    if (newp != nullptr) {
      EXPECT_EQ(newlen, sizeof(ssize_t));
      writtenValue = *static_cast<ssize_t*>(newp);
    }
    calls.push_back({name, writtenValue});
    if (name == failingName) {
      return 1;
    }
    if (std::string_view{name} == "arenas.narenas") {
      EXPECT_EQ(*oldlenp, sizeof(numArenas));
      std::memcpy(oldp, &numArenas, sizeof(numArenas));
    }
    return 0;
  };
}
}  // namespace

// Test which kernels may have the bug.
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
  EXPECT_FALSE(mayHaveBug("aarch64", "7.2.1"));
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

// Test the outcomes that don't call `mallctl`, and the overload without
// arguments.
TEST(KernelBugWorkaround, applyIfNeeded) {
  // Neither of these calls `mallctl`.
  std::vector<MallctlCall> calls;
  EXPECT_EQ(applyIfNeeded("x86_64", "7.0.0-38-generic", false,
                          makeMallctlMock(calls)),
            Status::NotNeeded);
  EXPECT_EQ(applyIfNeeded("aarch64", "7.2.0", false, makeMallctlMock(calls)),
            Status::NotNeeded);
  EXPECT_EQ(applyIfNeeded("aarch64", "7.0.0-1017-azure", true,
                          makeMallctlMock(calls)),
            Status::DisabledByUser);
  EXPECT_TRUE(calls.empty());
  // The overload without arguments must work on any machine.
  [[maybe_unused]] auto status = applyIfNeeded();
#ifndef __linux__
  // The workaround is only available on Linux.
  EXPECT_EQ(status, Status::NotNeeded);
#endif
}

// Test that the workaround is not available without `jemalloc`.
TEST(KernelBugWorkaround, noJemalloc) {
  EXPECT_EQ(applyIfNeeded("aarch64", "7.0.0-1017-azure", false, {}),
            Status::NoJemalloc);
}

// Test that a failure to set the default for new arenas is reported as
// `Status::NoJemalloc`, and that the existing arenas are then left alone.
TEST(KernelBugWorkaround, settingDefaultFails) {
  std::vector<MallctlCall> calls;
  EXPECT_EQ(applyIfNeeded("aarch64", "7.0.0-1017-azure", false,
                          makeMallctlMock(calls, "arenas.dirty_decay_ms", 3)),
            Status::NoJemalloc);
  EXPECT_THAT(calls,
              ::testing::ElementsAre(MallctlCall{"arenas.dirty_decay_ms", -1}));
}

// Test that the workaround is still applied (to new arenas) if the number of
// existing arenas can't be determined.
TEST(KernelBugWorkaround, numArenasFails) {
  std::vector<MallctlCall> calls;
  EXPECT_EQ(applyIfNeeded("aarch64", "7.0.0-1017-azure", false,
                          makeMallctlMock(calls, "arenas.narenas", 3)),
            Status::Applied);
  EXPECT_THAT(calls, ::testing::ElementsAre(
                         MallctlCall{"arenas.dirty_decay_ms", -1},
                         MallctlCall{"arenas.narenas", std::nullopt}));
}

// Test that the workaround is applied to the default for new arenas and to
// each existing arena, also if setting one of the existing arenas fails.
TEST(KernelBugWorkaround, appliedToAllArenas) {
  std::vector<MallctlCall> calls;
  EXPECT_EQ(applyIfNeeded("aarch64", "7.1.8", false,
                          makeMallctlMock(calls, "arena.1.dirty_decay_ms", 3)),
            Status::Applied);
  EXPECT_THAT(
      calls, ::testing::ElementsAre(MallctlCall{"arenas.dirty_decay_ms", -1},
                                    MallctlCall{"arenas.narenas", std::nullopt},
                                    MallctlCall{"arena.0.dirty_decay_ms", -1},
                                    MallctlCall{"arena.1.dirty_decay_ms", -1},
                                    MallctlCall{"arena.2.dirty_decay_ms", -1}));
}

#ifdef __linux__
// Test the workaround with the real `jemalloc` (if QLever uses it).
TEST(KernelBugWorkaround, realJemalloc) {
  if (mallctl == nullptr) {
    GTEST_SKIP() << "QLever doesn't use `jemalloc`.";
  }
  EXPECT_EQ(applyIfNeeded("aarch64", "7.0.0-1017-azure", false, &mallctl),
            Status::Applied);
  // The default for new arenas and the setting of arena 0 are now -1.
  ssize_t decay = 0;
  size_t size = sizeof(decay);
  ASSERT_EQ(mallctl("arenas.dirty_decay_ms", &decay, &size, nullptr, 0), 0);
  EXPECT_EQ(decay, -1);
  decay = 0;
  ASSERT_EQ(mallctl("arena.0.dirty_decay_ms", &decay, &size, nullptr, 0), 0);
  EXPECT_EQ(decay, -1);
}
#endif
