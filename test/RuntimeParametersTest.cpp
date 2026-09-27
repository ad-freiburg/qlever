// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "global/RuntimeParameters.h"
#include "util/GTestHelpers.h"

using ::testing::AllOf;
using ::testing::HasSubstr;

// Test setting a runtime parameter from a single `<name>=<value>` string, as
// used by the `--set-runtime-parameter` option of `qlever-server`.
TEST(RuntimeParameters, setFromAssignment) {
  RuntimeParameters params;

  // A valid assignment sets the parameter.
  params.setFromAssignment("default-query-timeout=300s");
  EXPECT_EQ(params.defaultQueryTimeout_.get(), std::chrono::seconds{300});

  // The string is split at the FIRST `=`, so values containing `=` work.
  params.setFromAssignment("default-query-timeout=150s");
  EXPECT_EQ(params.defaultQueryTimeout_.get(), std::chrono::seconds{150});

  // A missing `=` is rejected with a readable message.
  AD_EXPECT_THROW_WITH_MESSAGE(
      params.setFromAssignment("no-equals-sign"),
      HasSubstr("assignment of the form <name>=<value>"));

  // An unknown parameter name is rejected, and the message lists the
  // available parameters.
  AD_EXPECT_THROW_WITH_MESSAGE(
      params.setFromAssignment("no-such-parameter=42"),
      AllOf(HasSubstr("No parameter with name no-such-parameter"),
            HasSubstr("Available parameters are:"),
            HasSubstr("default-query-timeout")));

  // An invalid value for an existing parameter is rejected.
  AD_EXPECT_THROW_WITH_MESSAGE(
      params.setFromAssignment("default-query-timeout=banana"),
      HasSubstr("Could not set parameter default-query-timeout"));

  // The use case that prompted this option (see #3031): set the maximal
  // number of redirects for query federation at startup.
  params.setFromAssignment("service-max-redirects=5");
  EXPECT_EQ(params.serviceMaxRedirects_.get(), 5u);
}

// Test that the value `0` is rejected for `lazy-index-scan-num-threads`
// (the implementation of the lazy scans requires at least one thread).
TEST(RuntimeParameters, lazyIndexScanNumThreadsIsStrictlyPositive) {
  RuntimeParameters params;
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      params.setFromAssignment("lazy-index-scan-num-threads=0"),
      AllOf(HasSubstr("lazy-index-scan-num-threads"),
            HasSubstr("strictly positive")),
      std::runtime_error);
  EXPECT_NO_THROW(params.setFromAssignment("lazy-index-scan-num-threads=1"));
  EXPECT_EQ(params.lazyIndexScanNumThreads_.get(), 1u);
}

// Test that `getKeys` and `toMap` (the building blocks of
// `--set-runtime-parameter help`) are consistent with each other.
TEST(RuntimeParameters, getKeysAndToMapAreConsistent) {
  RuntimeParameters params;
  auto keys = params.getKeys();
  auto map = params.toMap();
  EXPECT_FALSE(keys.empty());
  EXPECT_EQ(keys.size(), map.size());
  for (const auto& key : keys) {
    EXPECT_TRUE(map.contains(key)) << key;
  }
}

// The io_uring tuning knobs default to the current behavior: a 256-slot
// ring, uncapped batches, and no SQPoll thread.
TEST(RuntimeParameters, ioUringKnobDefaults) {
  RuntimeParameters params;
  EXPECT_EQ(params.ioUringRingSize_.get(), 256u);
  EXPECT_EQ(params.vocabBatchWindow_.get(), 0u);
  EXPECT_FALSE(params.ioUringSqPoll_.get());
}

// The ring size must name a usable liburing ring: 0 and values above 4096
// are rejected, the boundaries are accepted.
TEST(RuntimeParameters, ioUringRingSizeConstraints) {
  RuntimeParameters params;
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      params.setFromAssignment("iouring-ring-size=0"),
      AllOf(HasSubstr("iouring-ring-size"), HasSubstr("1 and 4096")),
      std::runtime_error);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      params.setFromAssignment("iouring-ring-size=4097"),
      AllOf(HasSubstr("iouring-ring-size"), HasSubstr("1 and 4096")),
      std::runtime_error);
  EXPECT_NO_THROW(params.setFromAssignment("iouring-ring-size=1"));
  EXPECT_EQ(params.ioUringRingSize_.get(), 1u);
  EXPECT_NO_THROW(params.setFromAssignment("iouring-ring-size=4096"));
  EXPECT_EQ(params.ioUringRingSize_.get(), 4096u);
}

// The batch window is either uncapped (0) or a cap up to 1M reads.
TEST(RuntimeParameters, vocabBatchWindowConstraints) {
  RuntimeParameters params;
  EXPECT_NO_THROW(params.setFromAssignment("vocab-batch-window=0"));
  EXPECT_EQ(params.vocabBatchWindow_.get(), 0u);
  EXPECT_NO_THROW(params.setFromAssignment("vocab-batch-window=1000000"));
  EXPECT_EQ(params.vocabBatchWindow_.get(), 1'000'000u);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      params.setFromAssignment("vocab-batch-window=1000001"),
      AllOf(HasSubstr("vocab-batch-window"), HasSubstr("0 and 1000000")),
      std::runtime_error);
}

// The SQPoll toggle accepts plain booleans in both directions.
TEST(RuntimeParameters, ioUringSqPollToggle) {
  RuntimeParameters params;
  EXPECT_NO_THROW(params.setFromAssignment("iouring-sqpoll=true"));
  EXPECT_TRUE(params.ioUringSqPoll_.get());
  EXPECT_NO_THROW(params.setFromAssignment("iouring-sqpoll=false"));
  EXPECT_FALSE(params.ioUringSqPoll_.get());
}
