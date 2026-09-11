//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "./util/GTestHelpers.h"
#include "backports/filesystem.h"
#include "util/ExternalOverflowStorage.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/Serializer.h"

using ad_utility::ExternalOverflowStorage;

namespace {
// Collect all elements of the `storage` (in order) into a `std::vector`.
template <typename T>
std::vector<T> collect(ExternalOverflowStorage<T>& storage) {
  std::vector<T> result;
  storage.forEach([&result](const T& element) { result.push_back(element); });
  return result;
}

// A small type that is not trivially serializable, but has an explicit
// `serialize` function. Used to make sure that the `ExternalOverflowStorage`
// works with such types as well.
struct NonTrivial {
  int a_ = 0;
  std::string b_;
  bool operator==(const NonTrivial&) const = default;
  AD_SERIALIZE_FRIEND_FUNCTION(NonTrivial) {
    serializer | arg.a_;
    serializer | arg.b_;
  }
};
}  // namespace

// ___________________________________________________________________________
TEST(ExternalOverflowStorage, emptyStorage) {
  ExternalOverflowStorage<int> storage{5, gtestCurrentTestName()};
  EXPECT_THAT(collect(storage), ::testing::IsEmpty());
}

// ___________________________________________________________________________
TEST(ExternalOverflowStorage, allInMemory) {
  std::string filename = gtestCurrentTestName();
  ExternalOverflowStorage<int> storage{5, filename};
  std::vector<int> expected;
  // Push fewer elements than the threshold, so nothing overflows to disk.
  for (int i = 0; i < 5; ++i) {
    storage.push_back(i);
    expected.push_back(i);
  }
  EXPECT_THAT(collect(storage), ::testing::ElementsAreArray(expected));
  // No overflow file should have been created.
  EXPECT_FALSE(ql::filesystem::exists(filename));
}

// ___________________________________________________________________________
TEST(ExternalOverflowStorage, overflowToDisk) {
  ExternalOverflowStorage<int> storage{5, gtestCurrentTestName()};
  std::vector<int> expected;
  // Push more elements than the threshold, so the surplus overflows to disk.
  for (int i = 0; i < 100; ++i) {
    storage.push_back(i);
    expected.push_back(i);
  }
  EXPECT_THAT(collect(storage), ::testing::ElementsAreArray(expected));
  // `forEach` does not modify the contents, so it can be called repeatedly.
  EXPECT_THAT(collect(storage), ::testing::ElementsAreArray(expected));
}

// ___________________________________________________________________________
TEST(ExternalOverflowStorage, thresholdZeroEverythingOnDisk) {
  ExternalOverflowStorage<int> storage{0, gtestCurrentTestName()};
  std::vector<int> expected;
  for (int i = 0; i < 20; ++i) {
    storage.push_back(i);
    expected.push_back(i);
  }
  EXPECT_THAT(collect(storage), ::testing::ElementsAreArray(expected));
}

// ___________________________________________________________________________
TEST(ExternalOverflowStorage, clearAndReuse) {
  ExternalOverflowStorage<int> storage{5, gtestCurrentTestName()};
  for (int i = 0; i < 100; ++i) {
    storage.push_back(i);
  }
  storage.clear();
  EXPECT_THAT(collect(storage), ::testing::IsEmpty());

  // Reuse with different (and again overflowing) elements to make sure that the
  // overflow file is truncated and no stale elements remain.
  std::vector<int> expected;
  for (int i = 0; i < 50; ++i) {
    storage.push_back(i + 1000);
    expected.push_back(i + 1000);
  }
  EXPECT_THAT(collect(storage), ::testing::ElementsAreArray(expected));
}

// ___________________________________________________________________________
TEST(ExternalOverflowStorage, nonTriviallySerializableType) {
  ExternalOverflowStorage<NonTrivial> storage{3, gtestCurrentTestName()};
  std::vector<NonTrivial> expected;
  for (int i = 0; i < 30; ++i) {
    NonTrivial element{i, "string number " + std::to_string(i)};
    storage.push_back(element);
    expected.push_back(element);
  }
  EXPECT_THAT(collect(storage), ::testing::ElementsAreArray(expected));
}

// ___________________________________________________________________________
TEST(ExternalOverflowStorage, overflowFileIsDeletedOnDestruction) {
  std::string filename = gtestCurrentTestName();
  ASSERT_FALSE(ql::filesystem::exists(filename));
  {
    ExternalOverflowStorage<int> storage{5, filename};
    for (int i = 0; i < 100; ++i) {
      storage.push_back(i);
    }
    // The overflow file has been created.
    EXPECT_TRUE(ql::filesystem::exists(filename));
  }
  // ... and is deleted again when the storage is destroyed.
  EXPECT_FALSE(ql::filesystem::exists(filename));
}
