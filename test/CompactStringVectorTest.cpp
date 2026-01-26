//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gtest/gtest.h>

#include <algorithm>

#include "backports/iterator.h"
#include "global/Pattern.h"

namespace {
std::vector<std::string> strings{"alpha", "b", "3920193",
                                 "<Qlever-internal-langtag>"};
std::vector<std::string> strings1{"bi", "ba", "12butzemann",
                                  "<Qlever-internal-langtag>"};
std::vector<std::vector<int>> ints{{1, 2, 3}, {42}, {6, 5, -4, 96}, {-38, 0}};
std::vector<std::vector<int>> ints1{{1}, {42, 19}, {6, 5, -4, 96}, {-38, 4, 7}};

auto iterablesEqual(const auto& a, const auto& b) {
  ASSERT_EQ(a.size(), b.size());
  for (size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(a[i], b[i]);
  }
}

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

using CompactVectorChar = CompactVectorOfStrings<char>;
using CompactVectorInt = CompactVectorOfStrings<int>;
}  // namespace

TEST(CompactVectorOfStrings, Build) {
  CompactVectorInt i;
  i.build(ints);
  vectorsEqual(i, ints);

  CompactVectorChar s;
  s.build(strings);
  vectorsEqual(s, strings);
}

TEST(CompactVectorOfStrings, Iterator) {
  auto testIterator = [](const auto& v, const auto& input) {
    using V = std::decay_t<decltype(v)>;

    V s;
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
  };
  testIterator(CompactVectorChar{}, strings);
  testIterator(CompactVectorInt{}, ints);
}

TEST(CompactVectorOfStrings, IteratorCategory) {
  using It = CompactVectorOfStrings<char>::Iterator;
  static_assert(std::random_access_iterator<It>);
}

TEST(CompactVectorOfStrings, Serialization) {
  auto testSerialization = [](const auto& v, auto& input) {
    using V = std::decay_t<decltype(v)>;

    const std::string filename = "_writerTest1.dat";
    {
      V vector;
      vector.build(input);
      ad_utility::serialization::FileWriteSerializer ser{filename};
      ser << vector;
    }  // The destructor finishes writing the file.

    V vector;
    ad_utility::serialization::FileReadSerializer ser{filename};
    ser >> vector;

    vectorsEqual(input, vector);

    ad_utility::deleteFile(filename);
  };

  testSerialization(CompactVectorChar{}, strings);
  testSerialization(CompactVectorInt{}, ints);
}

TEST(CompactVectorOfStrings, SerializationWithPush) {
  auto testSerializationWithPush = [](const auto& v, auto& inputVector) {
    using V = std::decay_t<decltype(v)>;

    const std::string filename = "_writerTest2.dat";
    {
      typename V::Writer writer{filename};
      for (const auto& s : inputVector) {
        writer.push(s.data(), s.size());
      }
    }  // The constructor finishes writing the file.

    V compactVector;
    ad_utility::serialization::FileReadSerializer ser{filename};
    ser >> compactVector;

    vectorsEqual(inputVector, compactVector);

    ad_utility::deleteFile(filename);
  };
  testSerializationWithPush(CompactVectorChar{}, strings);
  testSerializationWithPush(CompactVectorInt{}, ints);
}

// Test that a `CompactStringVectorWriter` can be correctly move-constructed and
// move-assigned into an empty writer, even when writing has already started.
TEST(CompactVectorOfStrings, MoveIntoEmptyWriter) {
  auto testSerializationWithPush = [](const auto& v, auto& inputVector) {
    using V = std::decay_t<decltype(v)>;

    const std::string filename = "_writerTest1029348.dat";
    {
      // Move-assign and move-construct before pushing anything.
      typename V::Writer writer1{filename};
      auto writer0{std::move(writer1)};
      writer1 = std::move(writer0);

      std::optional<typename V::Writer> writer2;
      auto* writer = &writer1;
      size_t i = 0;
      for (const auto& s : inputVector) {
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

    V compactVector;
    ad_utility::serialization::FileReadSerializer ser{filename};
    ser >> compactVector;

    vectorsEqual(inputVector, compactVector);

    ad_utility::deleteFile(filename);
  };
  testSerializationWithPush(CompactVectorChar{}, strings);
  testSerializationWithPush(CompactVectorInt{}, ints);
}

// Test the special case of move-assigning a `CompactStringVectorWriter` where
// the target of the move has already been written to.
TEST(CompactVectorOfStrings, MoveIntoFullWriter) {
  auto testSerializationWithPush = [](const auto& v, const auto& input1,
                                      const auto& input2) {
    using V = std::decay_t<decltype(v)>;

    const std::string filename = "_writerTest1029348A.dat";
    const std::string filename2 = "_writerTest1029348B.dat";
    {
      // Move-assign and move-construct before pushing anything.
      typename V::Writer writer{filename};
      for (const auto& s : input1) {
        writer.push(s.data(), s.size());
      }

      typename V::Writer writer2{filename2};
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

    V compactVector;
    ad_utility::serialization::FileReadSerializer ser{filename};
    ser >> compactVector;

    vectorsEqual(input1, compactVector);
    ad_utility::serialization::FileReadSerializer ser2{filename2};
    ser2 >> compactVector;
    vectorsEqual(input2, compactVector);

    ad_utility::deleteFile(filename);
    ad_utility::deleteFile(filename2);
  };
  testSerializationWithPush(CompactVectorChar{}, strings, strings1);
  testSerializationWithPush(CompactVectorInt{}, ints, ints1);
}

TEST(CompactVectorOfStrings, SerializationWithPushMiddleOfFile) {
  auto testSerializationWithPush = [](const auto& v, auto& inputVector) {
    using V = std::decay_t<decltype(v)>;

    const std::string filename = "_writerTest3.dat";
    {
      ad_utility::serialization::FileWriteSerializer fileWriter{filename};
      fileWriter << 42;
      typename V::Writer writer{std::move(fileWriter).file()};
      for (const auto& s : inputVector) {
        writer.push(s.data(), s.size());
      }
      fileWriter =
          ad_utility::serialization::FileWriteSerializer{writer.finish()};
      fileWriter << -3;
    }

    V compactVector;
    ad_utility::serialization::FileReadSerializer ser{filename};
    int i = 0;
    ser >> i;
    ASSERT_EQ(42, i);
    ser >> compactVector;
    ser >> i;
    ASSERT_EQ(-3, i);

    vectorsEqual(inputVector, compactVector);

    ad_utility::deleteFile(filename);
  };
  testSerializationWithPush(CompactVectorChar{}, strings);
  testSerializationWithPush(CompactVectorInt{}, ints);
}
