//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/TriplesView.h"

auto V = [](const auto& id) { return Id::Vocab(id); };

// This struct mocks the structure of the actual `Permutation` types used in
// QLever for testing the `TriplesView`.
struct DummyPermutation {
  void scan(Id col0Id, auto* result) const {
    result->reserve(col0Id.getVocabUnchecked());
    for (size_t i = 0; i < col0Id.getVocabUnchecked(); ++i) {
      result->push_back(std::array{V((i + 1) * col0Id.getVocabUnchecked()),
                                   V((i + 2) * col0Id.getVocabUnchecked())});
    }
  }

  struct Metadata {
    struct Data {
      std::vector<Id> _ids{V(1), V(3), V(5), V(7), V(8), V(10), V(13)};
      using BaseIterator = std::vector<Id>::const_iterator;
      struct Iterator : BaseIterator {
        Iterator(BaseIterator b) : BaseIterator{std::move(b)} {}
        static Id getIdFromElement(Id id) { return id; }
        Id getId() const { return *(*this); }
      };
      Iterator ordered_begin() const { return _ids.begin(); }
      Iterator ordered_end() const { return _ids.end(); }
    };
    Data data() const { return {}; }
  };

  Metadata _meta;
};

auto allocator = ad_utility::AllocatorWithLimit<Id>{
    ad_utility::makeAllocationMemoryLeftThreadsafeObject(10000)};

std::vector<std::array<Id, 3>> expectedResult() {
  std::vector<Id> ids{V(1), V(3), V(5), V(7), V(8), V(10), V(13)};
  std::vector<std::array<Id, 3>> result;
  for (auto idComplex : ids) {
    auto id = idComplex.getVocabUnchecked();
    for (size_t i = 0; i < id; ++i) {
      result.push_back(std::array{V(id), V(id * (i + 1)), V(id * (i + 2))});
    }
  }
  return result;
}

TEST(TriplesView, AllTriples) {
  std::vector<std::array<Id, 3>> result;
  for (auto triple : TriplesView(DummyPermutation{}, allocator)) {
    result.push_back(triple);
  }
  ASSERT_EQ(result, expectedResult());
}

TEST(TriplesView, IgnoreRanges) {
  std::vector<std::array<Id, 3>> result;
  auto expected = expectedResult();
  std::erase_if(expected, [](const auto& triple) {
    auto t = triple[0].getVocabUnchecked();
    return t == 1 || t == 3 || t == 7 || t == 13;
  });
  std::vector<std::pair<Id, Id>> ignoredRanges{
      {V(0), V(4)}, {V(7), V(8)}, {V(13), V(87593)}};
  for (auto triple :
       TriplesView(DummyPermutation{}, allocator, ignoredRanges)) {
    result.push_back(triple);
  }
  ASSERT_EQ(result, expected);
}

TEST(TriplesView, IgnoreTriples) {
  std::vector<std::array<Id, 3>> result;
  auto expected = expectedResult();
  auto isTripleIgnored = [](const auto& triple) {
    return triple[1].getVocabUnchecked() % 2 == 0;
  };
  std::erase_if(expected, isTripleIgnored);
  for (auto triple :
       TriplesView(DummyPermutation{}, allocator, {}, isTripleIgnored)) {
    result.push_back(triple);
  }
  ASSERT_EQ(result, expected);
}
