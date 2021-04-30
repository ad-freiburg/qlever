//
// Created by johannes on 01.05.21.
//

#include <gtest/gtest.h>

#include "../src/util/Random.h"
#include "../src/util/Serializer/FileSerializer.h"
#include "../src/util/Serializer/SerializeHashMap.h"
#include "../src/util/Serializer/Serializer.h"

using namespace ad_utility;
using namespace ad_utility::serialization;

auto testWithByteBuffer = [](auto testFunction) {
  ByteBufferWriteSerializer writer;
  auto makeReader = [&writer]() {
    return ByteBufferReadSerializer{std::move(writer).data()};
  };
  testFunction(writer, makeReader);
};

auto testWithFileSerialization = [](auto testFunction) {
  const std::string filename = "serializationTestTmpt.txtx";
  FileWriteSerializer writer{filename};
  auto makeReader = [filename, &writer] {
    writer.close();
    return FileReadSerializer{filename};
  };
  testFunction(writer, makeReader);
  unlink(filename.c_str());
};

auto testWithAllSerializers = [](auto testFunction) {
  testWithByteBuffer(testFunction);
  testWithFileSerialization(testFunction);
  // TODO<joka921> Register new serializers here to apply all existing tests
  // to them
};

TEST(Serializer, Simple) {
  auto simpleIntTest = [](auto& writer, auto makeReader) {
    int x = 42;
    writer& x;

    auto reader = makeReader();
    int y;
    reader& y;
    ASSERT_EQ(y, 42);
  };
  testWithAllSerializers(simpleIntTest);
}
TEST(Serializer, ManyTrivialDatatypes) {
  auto testmanyPrimitives = [](auto&& writer, auto makeReader) {
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
      writer& ints.back();
      chars.push_back(static_cast<char>(r()));
      writer& chars.back();
      shorts.push_back(static_cast<short>(r()));
      writer& shorts.back();
      longInts.push_back(static_cast<int64_t>(r()));
      writer& longInts.back();
      doubles.push_back(d());
      writer& doubles.back();
      floats.push_back(static_cast<float>(d()));
      writer& floats.back();
    }

    auto reader = makeReader();
    for (int i = 0; i < numIterations; ++i) {
      int x;
      reader& x;
      ASSERT_EQ(x, ints[i]);
      char c;
      reader& c;
      ASSERT_EQ(c, chars[i]);
      short s;
      reader& s;
      ASSERT_EQ(s, shorts[i]);
      int64_t l;
      reader& l;
      ASSERT_EQ(l, longInts[i]);
      double dob;
      reader& dob;
      ASSERT_FLOAT_EQ(dob, doubles[i]);
      float f;
      reader& f;
      ASSERT_FLOAT_EQ(f, floats[i]);
    }
  };

  testWithAllSerializers(testmanyPrimitives);
}