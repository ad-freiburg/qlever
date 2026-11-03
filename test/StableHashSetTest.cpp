// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/hash/hash.h>
#include <gmock/gmock.h>

#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "util/StableHashSet.h"

namespace {

using ad_utility::StableHashSet;

// Hash and compare `std::string`s and `std::string_view`s by their content.
struct Hash {
  using is_transparent = void;
  size_t operator()(std::string_view s) const {
    return absl::Hash<std::string_view>{}(s);
  }
};
struct Equal {
  using is_transparent = void;
  bool operator()(std::string_view a, std::string_view b) const {
    return a == b;
  }
};

using StringSet = StableHashSet<std::string, Hash, Equal>;
using ViewSet = StableHashSet<std::string_view, Hash, Equal>;

// Insert `word` into a `ViewSet`, such that its characters are stored in the
// arena of the set.
std::pair<const std::string_view*, bool> insertView(ViewSet& set,
                                                    std::string_view word) {
  return set.insertWith(word, [word](ql::pmr::memory_resource& arena) {
    auto* characters =
        static_cast<char*>(arena.allocate(word.size(), alignof(char)));
    ql::ranges::copy(word, characters);
    return std::string_view{characters, word.size()};
  });
}

// A memory resource that throws on the `failAt_`-th call to `allocate` (and
// on all later calls until `failAt_` is changed).
class ThrowingResource : public ql::pmr::memory_resource {
 public:
  size_t numAllocations_ = 0;
  size_t failAt_ = std::numeric_limits<size_t>::max();

 private:
  void* do_allocate(size_t bytes, size_t alignment) override {
    if (++numAllocations_ >= failAt_) {
      throw std::runtime_error{"limit exceeded"};
    }
    return ql::pmr::new_delete_resource()->allocate(bytes, alignment);
  }
  void do_deallocate(void* p, size_t bytes, size_t alignment) override {
    ql::pmr::new_delete_resource()->deallocate(p, bytes, alignment);
  }
  bool do_is_equal(const memory_resource& other) const noexcept override {
    return this == &other;
  }
};

}  // namespace

// _____________________________________________________________________________
TEST(StableHashSet, insertFindAndIterate) {
  StringSet set;
  EXPECT_TRUE(set.empty());
  EXPECT_EQ(set.find(std::string_view{"a"}), nullptr);

  auto [a, aIsNew] = set.insert("a");
  EXPECT_TRUE(aIsNew);
  EXPECT_EQ(*a, "a");
  auto [a2, a2IsNew] = set.insert("a");
  EXPECT_FALSE(a2IsNew);
  EXPECT_EQ(a2, a);
  EXPECT_EQ(set.size(), 1);
  EXPECT_FALSE(set.empty());

  // Lookup via a different key type.
  EXPECT_EQ(set.find(std::string_view{"a"}), a);
  EXPECT_EQ(set.find(std::string_view{"b"}), nullptr);

  // The elements never move, also when the index grows.
  std::vector<std::pair<const std::string*, std::string>> elements;
  for (size_t i = 0; i < 10'000; ++i) {
    std::string word = std::to_string(i);
    auto [element, isNew] = set.insert(word);
    EXPECT_TRUE(isNew);
    elements.emplace_back(element, word);
  }
  EXPECT_EQ(set.size(), 10'001);
  for (const auto& [element, word] : elements) {
    EXPECT_EQ(*element, word);
    EXPECT_EQ(set.find(std::string_view{word}), element);
  }
  EXPECT_EQ(*a, "a");

  // Iteration visits each element exactly once.
  std::vector<std::string> visited{set.begin(), set.end()};
  std::vector<std::string> expected;
  expected.push_back("a");
  for (const auto& [element, word] : elements) {
    expected.push_back(word);
  }
  EXPECT_THAT(visited, ::testing::UnorderedElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(StableHashSet, insertWithStoresAdditionalDataInTheArena) {
  ViewSet set;
  std::string word = "someWord";
  auto [element, isNew] = insertView(set, word);
  EXPECT_TRUE(isNew);
  EXPECT_EQ(*element, "someWord");
  // The characters are a copy in the arena.
  EXPECT_NE(element->data(), word.data());
  word = "overwritten";
  EXPECT_EQ(*element, "someWord");

  // For an equal key, `makeElement` is not called.
  bool called = false;
  auto [existing, existingIsNew] = set.insertWith(
      std::string_view{"someWord"}, [&called](ql::pmr::memory_resource&) {
        called = true;
        return std::string_view{"someWord"};
      });
  EXPECT_FALSE(existingIsNew);
  EXPECT_EQ(existing, element);
  EXPECT_FALSE(called);
}

// _____________________________________________________________________________
TEST(StableHashSet, failingMakeElementLeavesSetUnchanged) {
  ViewSet set;
  insertView(set, "a");
  EXPECT_THROW(
      set.insertWith(std::string_view{"b"},
                     [](ql::pmr::memory_resource&) -> std::string_view {
                       throw std::runtime_error{"failed"};
                     }),
      std::runtime_error);
  EXPECT_EQ(set.size(), 1);
  EXPECT_EQ(set.find(std::string_view{"b"}), nullptr);
  EXPECT_TRUE(insertView(set, "b").second);
  EXPECT_EQ(set.size(), 2);
}

// _____________________________________________________________________________
TEST(StableHashSet, failingAllocationsLeaveSetUnchanged) {
  static constexpr size_t numWords = 3'000;
  auto word = [](size_t i) {
    return std::string(i % 50, 'x') + std::to_string(i);
  };

  // Count the allocations of a complete run, then let each of them fail once.
  // This covers failing allocations of the arena (for the elements and their
  // characters) as well as of the index (when it grows).
  size_t totalAllocations = 0;
  {
    ThrowingResource resource;
    ViewSet set{&resource};
    for (size_t i = 0; i < numWords; ++i) {
      insertView(set, word(i));
    }
    totalAllocations = resource.numAllocations_;
  }
  ASSERT_GT(totalAllocations, 5);

  for (size_t failAt = 1; failAt <= totalAllocations; ++failAt) {
    ThrowingResource resource;
    resource.failAt_ = failAt;
    ViewSet set{&resource};
    size_t numInserted = 0;
    try {
      for (; numInserted < numWords; ++numInserted) {
        insertView(set, word(numInserted));
      }
      FAIL() << "No allocation failed for failAt = " << failAt;
    } catch (const std::runtime_error&) {
    }
    // The failed insertion had no effect.
    ASSERT_EQ(set.size(), numInserted) << failAt;
    EXPECT_EQ(set.find(std::string_view{word(numInserted)}), nullptr);
    for (size_t i = 0; i < numInserted; ++i) {
      ASSERT_NE(set.find(std::string_view{word(i)}), nullptr) << failAt;
    }
    // After the limit is lifted, the set is fully functional.
    resource.failAt_ = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < numWords; ++i) {
      auto [element, isNew] = insertView(set, word(i));
      EXPECT_EQ(isNew, i >= numInserted);
      EXPECT_EQ(*element, word(i));
    }
    ASSERT_EQ(set.size(), numWords);
    ASSERT_EQ(static_cast<size_t>(std::distance(set.begin(), set.end())),
              numWords);
  }
}

// _____________________________________________________________________________
TEST(StableHashSet, nonTriviallyDestructibleElements) {
  // The elements are destroyed together with the set (the leak checker of a
  // sanitizer build would complain otherwise). The strings are long enough to
  // not fit into the small-string buffer.
  auto set = std::make_unique<StringSet>();
  for (size_t i = 0; i < 100; ++i) {
    set->insert(std::string(100, 'x') + std::to_string(i));
  }
  EXPECT_EQ(set->size(), 100);
  set.reset();
}
