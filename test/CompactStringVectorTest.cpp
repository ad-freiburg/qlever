//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gmock/gmock.h>

#include <algorithm>

#include "backports/iterator.h"
#include "global/Pattern.h"

namespace {

// _____________________________________________________________________________
auto iterablesEqual(const auto& a, const auto& b) {
  ASSERT_EQ(a.size(), b.size());
  for (size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(a[i], b[i]);
  }
}

// _____________________________________________________________________________
auto vectorsEqual = [](const auto& compactVector, const auto& compareVector) {
  ASSERT_EQ(compactVector.size(), compareVector.size());
  for (size_t i = 0; i < compactVector.size(); ++i) {
    using value_type =
        typename std::decay_t<decltype(compareVector)>::value_type;
    value_type a = [&]() {
      return value_type(compactVector[i].begin(), compactVector[i].end());
    }();
    iterablesEqual(a, compareVector[i]);
  }
};

// Type traits helper to get test data for each type.
template <typename T>
struct TestData;

// _____________________________________________________________________________
template <>
struct TestData<char> {
  using CompactVector = CompactVectorOfStrings<char>;
  using InputType = std::vector<std::string>;
  static InputType input() {
    return {"alpha", "b", "3920193", "<Qlever-internal-langtag>"};
  }
  static InputType input1() {
    return {"bi", "ba", "12butzemann", "<Qlever-internal-langtag>"};
  }
};

// _____________________________________________________________________________
template <>
struct TestData<int> {
  using CompactVector = CompactVectorOfStrings<int>;
  using InputType = std::vector<std::vector<int>>;
  static InputType input() {
    return {{1, 2, 3}, {42}, {6, 5, -4, 96}, {-38, 0}};
  }
  static InputType input1() {
    return {{1}, {42, 19}, {6, 5, -4, 96}, {-38, 4, 7}};
  }
};

}  // namespace

// _____________________________________________________________________________
template <typename T>
class CompactVectorOfStringsFixture : public ::testing::Test {
 protected:
  using Type = T;
  using Data = TestData<T>;
  using CompactVector = typename Data::CompactVector;

  static inline const typename Data::InputType input_ = Data::input();
  static inline const typename Data::InputType input1_ = Data::input1();
};

// _____________________________________________________________________________
using TestTypes = ::testing::Types<char, int>;
TYPED_TEST_SUITE(CompactVectorOfStringsFixture, TestTypes);

// _____________________________________________________________________________
TYPED_TEST(CompactVectorOfStringsFixture, Build) {
  const auto& input = TestFixture::input_;
  typename TestFixture::CompactVector v;
  v.build(input);
  vectorsEqual(v, input);
}

// _____________________________________________________________________________
TYPED_TEST(CompactVectorOfStringsFixture, Iterator) {
  const auto& input = TestFixture::input_;
  typename TestFixture::CompactVector s;
  s.build(input);

  auto it = s.begin();
  using ql::ranges::equal;
  ASSERT_TRUE(equal(input[0], *it));
  ASSERT_TRUE(equal(input[0], *it++));
  ASSERT_TRUE(equal(input[1], *it));
  ASSERT_TRUE(equal(input[2], *++it));
  ASSERT_TRUE(equal(input[2], *it));
  ASSERT_TRUE(equal(input[2], *it--));
  ASSERT_TRUE(equal(input[1], *it));
  ASSERT_TRUE(equal(input[0], *--it));

  ASSERT_TRUE(equal(input[2], it[2]));
  ASSERT_TRUE(equal(input[2], *(it + 2)));
  ASSERT_TRUE(equal(input[2], *(2 + it)));
  ASSERT_TRUE(equal(input[3], *(it += 3)));
  ASSERT_TRUE(equal(input[2], *(it += -1)));
  ASSERT_TRUE(equal(input[2], *(it)));
  ASSERT_TRUE(equal(input[0], *(it -= 2)));
  ASSERT_TRUE(equal(input[2], *(it -= -2)));
  ASSERT_TRUE(equal(input[2], *(it)));

  it = s.end() + (-1);
  ASSERT_TRUE(equal(input.back(), *it));

  ASSERT_EQ(static_cast<int64_t>(s.size()), s.end() - s.begin());
}

// _____________________________________________________________________________
TEST(CompactVectorOfStrings, IteratorCategory) {
  using It = CompactVectorOfStrings<char>::Iterator;
  static_assert(std::random_access_iterator<It>);
}

// _____________________________________________________________________________
TYPED_TEST(CompactVectorOfStringsFixture, Serialization) {
  const auto& input = TestFixture::input_;
  using CompactVector = typename TestFixture::CompactVector;

  const std::string filename = "_writerTest1.dat";
  {
    CompactVector vector;
    vector.build(input);
    ad_utility::serialization::FileWriteSerializer ser{filename};
    ser << vector;
  }  // The destructor finishes writing the file.

  CompactVector vector;
  ad_utility::serialization::FileReadSerializer ser{filename};
  ser >> vector;

  vectorsEqual(input, vector);

  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
TYPED_TEST(CompactVectorOfStringsFixture, SerializationWithPush) {
  const auto& input = TestFixture::input_;
  using CompactVector = typename TestFixture::CompactVector;

  const std::string filename = "_writerTest2.dat";
  {
    typename CompactVector::Writer writer{filename};
    for (const auto& s : input) {
      writer.push(s.data(), s.size());
    }
  }  // The constructor finishes writing the file.

  CompactVector compactVector;
  ad_utility::serialization::FileReadSerializer ser{filename};
  ser >> compactVector;

  vectorsEqual(input, compactVector);

  ad_utility::deleteFile(filename);
}

// Test that a `CompactStringVectorWriter` can be correctly move-constructed and
// move-assigned into an empty writer, even when writing has already started.
TYPED_TEST(CompactVectorOfStringsFixture, MoveIntoEmptyWriter) {
  const auto& input = TestFixture::input_;
  using CompactVector = typename TestFixture::CompactVector;

  const std::string filename = "_writerTest1029348.dat";
  {
    // Move-assign and move-construct before pushing anything.
    typename CompactVector::Writer writer1{filename};
    auto writer0{std::move(writer1)};
    writer1 = std::move(writer0);

    std::optional<typename CompactVector::Writer> writer2;
    auto* writer = &writer1;
    size_t i = 0;
    for (const auto& s : input) {
      if (i == 1) {
        // Move assignment after push.
        writer0 = std::move(*writer);
        writer = &writer0;
      }
      if (i == 2) {
        // Move construction after push.
        writer2.emplace(std::move(*writer));
        writer = &writer2.value();
      }
      writer->push(s.data(), s.size());
      ++i;
    }
  }  // The constructor finishes writing the file.

  CompactVector compactVector;
  ad_utility::serialization::FileReadSerializer ser{filename};
  ser >> compactVector;

  vectorsEqual(input, compactVector);

  ad_utility::deleteFile(filename);
}

// Test the special case of move-assigning a `CompactStringVectorWriter` where
// the target of the move has already been written to.
TYPED_TEST(CompactVectorOfStringsFixture, MoveIntoFullWriter) {
  const auto& input1 = TestFixture::input_;
  const auto& input2 = TestFixture::input1_;
  using CompactVector = typename TestFixture::CompactVector;

  const std::string filename = "_writerTest1029348A.dat";
  const std::string filename2 = "_writerTest1029348B.dat";
  {
    // Move-assign and move-construct before pushing anything.
    typename CompactVector::Writer writer{filename};
    for (const auto& s : input1) {
      writer.push(s.data(), s.size());
    }

    typename CompactVector::Writer writer2{filename2};
    AD_CORRECTNESS_CHECK(input1.size() > 1);
    AD_CORRECTNESS_CHECK(input2.size() > 1);
    auto& fst = input2.at(0);
    writer2.push(fst.data(), fst.size());

    // Move the writer, both of the involved writers already have been written
    // to.
    writer = std::move(writer2);
    for (size_t i = 1; i < input2.size(); ++i) {
      auto& el = input2.at(i);
      writer.push(el.data(), el.size());
    }
  }

  CompactVector compactVector;
  ad_utility::serialization::FileReadSerializer ser{filename};
  ser >> compactVector;

  vectorsEqual(input1, compactVector);
  ad_utility::serialization::FileReadSerializer ser2{filename2};
  ser2 >> compactVector;
  vectorsEqual(input2, compactVector);

  ad_utility::deleteFile(filename);
  ad_utility::deleteFile(filename2);
}

// _____________________________________________________________________________
TYPED_TEST(CompactVectorOfStringsFixture, SerializationWithPushMiddleOfFile) {
  const auto& input = TestFixture::input_;
  using CompactVector = typename TestFixture::CompactVector;

  const std::string filename = "_writerTest3.dat";
  {
    ad_utility::serialization::FileWriteSerializer fileWriter{filename};
    fileWriter << 42;
    typename CompactVector::Writer writer{std::move(fileWriter).file()};
    for (const auto& s : input) {
      writer.push(s.data(), s.size());
    }
    fileWriter =
        ad_utility::serialization::FileWriteSerializer{writer.finish()};
    fileWriter << -3;
  }

  CompactVector compactVector;
  ad_utility::serialization::FileReadSerializer ser{filename};
  int i = 0;
  ser >> i;
  ASSERT_EQ(42, i);
  ser >> compactVector;
  ser >> i;
  ASSERT_EQ(-3, i);

  vectorsEqual(input, compactVector);

  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
TYPED_TEST(CompactVectorOfStringsFixture, cloneAndRemap) {
  const auto& input = TestFixture::input_;
  using CompactVector = typename TestFixture::CompactVector;

  CompactVector original;
  // Try with empty vector.
  auto copy0 = original.cloneAndRemap(std::identity{});
  EXPECT_TRUE(ql::ranges::equal(original, copy0, ql::ranges::equal));

  original.build(input);

  auto copy1 = original.cloneAndRemap(std::identity{});
  EXPECT_TRUE(ql::ranges::equal(original, copy1, ql::ranges::equal));

  auto mappingFunction = [](auto x) -> TestFixture::Type { return x + 1; };

  auto copy2 = original.cloneAndRemap(mappingFunction);

  ASSERT_EQ(original.size(), copy2.size());
  for (auto [reference, element] : ::ranges::zip_view(original, copy2)) {
    ASSERT_EQ(reference.size(), element.size());
    auto modifiedReference = ::ranges::to<typename CompactVector::vector_type>(
        reference | ql::views::transform(mappingFunction));
    EXPECT_THAT(modifiedReference, ::testing::ElementsAreArray(element));
  }
}
