//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/util/Random.h"
#include "../src/util/Serializer/FileSerializer.h"
#include "../src/util/Serializer/SerializeHashMap.h"
#include "../src/util/Serializer/SerializeString.h"
#include "../src/util/Serializer/SerializeVector.h"
#include "../src/util/Serializer/Serializer.h"

using namespace ad_utility;
using namespace ad_utility::serialization;

auto testWithByteBuffer = [](auto testFunction) {
  ByteBufferWriteSerializer writer;
  auto makeReaderFromWriter = [&writer]() {
    return ByteBufferReadSerializer{std::move(writer).data()};
  };
  testFunction(writer, makeReaderFromWriter);
};

auto testWithFileSerialization = [](auto testFunction) {
  const std::string filename = "serializationTest.tmp";
  FileWriteSerializer writer{filename};
  auto makeReaderFromWriter = [filename, &writer] {
    writer.close();
    return FileReadSerializer{filename};
  };
  testFunction(writer, makeReaderFromWriter);
  unlink(filename.c_str());
};

auto testWithAllSerializers = [](auto testFunction) {
  testWithByteBuffer(testFunction);
  testWithFileSerialization(testFunction);
  // TODO<joka921> Register new serializers here to apply all existing tests
  // to them
};

TEST(Serializer, Simple) {
  auto simpleIntTest = [](auto& writer, auto makeReaderFromWriter) {
    int x = 42;
    writer << x;

    auto reader = makeReaderFromWriter();
    int y;
    reader >> y;
    ASSERT_EQ(y, 42);
  };
  testWithAllSerializers(simpleIntTest);
}
TEST(Serializer, ManyTrivialDatatypes) {
  auto testmanyPrimitives = [](auto&& writer, auto makeReaderFromWriter) {
    FastRandomIntGenerator<size_t> r;
    RandomDoubleGenerator d;
    std::vector<int32_t> ints;
    std::vector<char> chars;
    std::vector<short> shorts;
    std::vector<int64_t> longInts;
    std::vector<double> doubles;
    std::vector<float> floats;

    const int numIterations = 300'000;

    for (int i = 0; i < numIterations; ++i) {
      ints.push_back(static_cast<int>(r()));
      writer | ints.back();
      chars.push_back(static_cast<char>(r()));
      writer | chars.back();
      shorts.push_back(static_cast<short>(r()));
      writer | shorts.back();
      longInts.push_back(static_cast<int64_t>(r()));
      writer | longInts.back();
      doubles.push_back(d());
      writer | doubles.back();
      floats.push_back(static_cast<float>(d()));
      writer | floats.back();
    }

    auto reader = makeReaderFromWriter();
    for (int i = 0; i < numIterations; ++i) {
      int x;
      reader | x;
      ASSERT_EQ(x, ints[i]);
      char c;
      reader | c;
      ASSERT_EQ(c, chars[i]);
      short s;
      reader | s;
      ASSERT_EQ(s, shorts[i]);
      int64_t l;
      reader | l;
      ASSERT_EQ(l, longInts[i]);
      double dob;
      reader | dob;
      ASSERT_FLOAT_EQ(dob, doubles[i]);
      float f;
      reader | f;
      ASSERT_FLOAT_EQ(f, floats[i]);
    }
  };

  testWithAllSerializers(testmanyPrimitives);
}

TEST(Serializer, StringAndHashMap) {
  auto testFunction = [](auto&& writer, auto makeReaderFromWriter) {
    ad_utility::HashMap<string, int> m;
    m["hallo"] = 42;
    m["tschüss"] = 84;

    std::string withZero = "something";
    withZero.push_back(0);
    withZero.push_back('a');
    m["withZero"] = 4321;
    writer | m;

    auto reader = makeReaderFromWriter();
    ad_utility::HashMap<string, int> n;
    reader | n;
    ASSERT_EQ(m, n);
  };
  testWithAllSerializers(testFunction);
}

TEST(Serializer, Vector) {
  auto testTriviallyCopyableDatatype = [](auto&& writer,
                                          auto makeReaderFromWriter) {
    std::vector<int> v{5, 6, 89, 42, -23948165, 0, 59309289, -42};
    writer | v;

    auto reader = makeReaderFromWriter();
    std::vector<int> w;
    reader | w;
    ASSERT_EQ(v, w);
  };

  auto testNonTriviallyCopyableDatatype = [](auto&& writer,
                                             auto makeReaderFromWriter) {
    std::vector<std::string> v{"hi",          "bye",      "someone",
                               "someoneElse", "23059178", "-42"};
    writer | v;

    auto reader = makeReaderFromWriter();
    std::vector<std::string> w;
    reader | w;
    ASSERT_EQ(v, w);
  };

  testWithAllSerializers(testTriviallyCopyableDatatype);
  testWithAllSerializers(testNonTriviallyCopyableDatatype);
}

TEST(Serializer, CopyAndMove) {
  auto testWithMove = [](auto&& writer, auto makeReaderFromWriter) {
    using Writer = std::decay_t<decltype(writer)>;

    // Assert that write serializers cannot be copied.
    static_assert(!requires(Writer w1, Writer w2) { {w1 = w2}; });
    static_assert(!std::constructible_from<Writer, const Writer&>);

    // Assert that moving writers consistently writes to the same resource.
    writer | 1;
    Writer writer2(std::move(writer));
    writer2 | 2;
    writer = std::move(writer2);
    writer | 3;

    auto reader = makeReaderFromWriter();
    // Assert that read serializers cannot be copied.
    using Reader = decltype(reader);
    static_assert(!requires(Reader r1, Reader r2) { {r1 = r2}; });
    static_assert(!std::constructible_from<Reader, const Reader&>);
    // Assert that moving writers consistently reads from the same resource.
    int i;
    reader | i;
    ASSERT_EQ(i, 1);
    decltype(reader) reader2{std::move(reader)};
    reader2 | i;
    ASSERT_EQ(i, 2);
    reader = std::move(reader2);
    reader | i;
    ASSERT_EQ(i, 3);
  };
  testWithAllSerializers(testWithMove);
}
