//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "backports/span.h"
#include "util/GTestHelpers.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Random.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/CompressedSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/FromCallableSerializer.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/Serializer/SerializeHashMap.h"
#include "util/Serializer/SerializeOptional.h"
#include "util/Serializer/SerializePair.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"

using namespace ad_utility;
using ad_utility::serialization::ByteBufferReadSerializer;
using ad_utility::serialization::ByteBufferWriteSerializer;
using ad_utility::serialization::CompressedReadSerializer;
using ad_utility::serialization::CompressedWriteSerializer;
using ad_utility::serialization::CopyableFileReadSerializer;
using ad_utility::serialization::FileReadSerializer;
using ad_utility::serialization::FileWriteSerializer;
using ad_utility::serialization::VectorIncrementalSerializer;

using ad_utility::serialization::ReadSerializer;
using ad_utility::serialization::ReadViaCallableSerializer;
using ad_utility::serialization::Serializer;
using ad_utility::serialization::WriteSerializer;
using ad_utility::serialization::WriteViaCallableSerializer;

// The following tests are also examples for the serialization module and for
// several pitfalls.
namespace testNamespaceA {
// Free serialization function.
struct A {
  int a;
  int b;
};
AD_SERIALIZE_FUNCTION(A) {
  serializer | arg.a;
  serializer | arg.b;
}

// Friend serialization function, defined inline.
class B {
 private:
  int a;
  int b;

 public:
  AD_SERIALIZE_FRIEND_FUNCTION(B) {
    serializer | arg.a;
    serializer | arg.b;
  }
};

// Friend serialization function, defined outside the class
class C {
 private:
  int a;
  int b;

 public:
  AD_SERIALIZE_FRIEND_FUNCTION(C) {
    serializer | arg.a;
    serializer | arg.b;
  }
};

// D is not serializable
struct D {};

struct E {
  D d;
  AD_SERIALIZE_FRIEND_FUNCTION(E) {
    // Ill-formed, because `D` has no `serialize()` function.
    serializer | arg.d;
  }
};

struct F {
  int a;
};
struct G {
  int a;
};
}  // namespace testNamespaceA

namespace testNamespaceB {
// `F is not serializable because the serialization function is not in
// F's namespace (or a parent namespace thereof)  nor in
// `ad_utility::serialization`
AD_SERIALIZE_FUNCTION(testNamespaceA::F) { serializer | arg.a; }
}  // namespace testNamespaceB

namespace ad_utility::serialization {
// G is now serializable (the serialize function is in the
// `ad_utility::serialization` namespace).
AD_SERIALIZE_FUNCTION(testNamespaceA::G) { serializer | arg.a; }
}  // namespace ad_utility::serialization

// Test that the claims about serializability are in fact true.
template <typename T>
static constexpr bool isReadSerializable =
    requires(ByteBufferReadSerializer s, T& t) { serialize(s, t); };
template <typename T>
static constexpr bool isWriteSerializable =
    requires(ByteBufferWriteSerializer s, T t) { serialize(s, t); };

// Simple dummy "compression" for testing (modify the data in a way that is
// simple, and reversed in the below dummy decompression function)
auto dummyCompress = [](ql::span<const char> data,
                        ad_utility::serialization::UninitializedBuffer& res) {
  res.clear();
  res.insert(res.end(), data.begin(), data.end());
  for (auto& c : res) {
    ++c;
  }
  res.push_back('A');
};
auto dummyDecompress = [](ql::span<const char> data, ql::span<char> res) {
  AD_CORRECTNESS_CHECK(res.size() == data.size() - 1);
  AD_CORRECTNESS_CHECK(data.back() == 'A');
  std::copy(data.begin(), data.end() - 1, res.begin());
  for (auto& c : res) {
    --c;
  }
};

TEST(Serializer, Serializability) {
  using testNamespaceA::A;
  using testNamespaceA::B;
  using testNamespaceA::C;
  using testNamespaceA::D;
  using testNamespaceA::E;
  using testNamespaceA::F;
  using testNamespaceA::G;
  static_assert(isReadSerializable<A>);
  static_assert(isWriteSerializable<A>);

  // We can serialize to and from any kind of a::A value or reference, but we
  // can only read from a serializer if the target is not const.
  static_assert(isReadSerializable<A&&>);
  static_assert(isWriteSerializable<A&&>);

  static_assert(isWriteSerializable<const A&>);
  static_assert(!isReadSerializable<const A&>);

  static_assert(isWriteSerializable<const A&&>);
  static_assert(!isReadSerializable<const A&&>);

  static_assert(isWriteSerializable<const A>);
  static_assert(!isReadSerializable<const A>);

  // See the definitions above as for why or why not these are serializable.
  static_assert(isReadSerializable<B>);
  // C is not read serializable on clang 17, probably because of a bug in
  // relation with templated friend functions which are defined outside the
  // class. This bug has been reported at
  // https://github.com/llvm/llvm-project/issues/71595
#if !(defined(__clang__) && (__clang_major__ == 17))
  static_assert(isReadSerializable<C>);
#endif
  static_assert(!isReadSerializable<D>);
  static_assert(!isReadSerializable<F>);
  static_assert(isReadSerializable<G>);

  // E seems to be serializable according to this SFINAE check, but
  // actually instantiating the serialize function wouldn't compile because
  // one of the members is not serializable.
  static_assert(isReadSerializable<E>);
}

// A simple example that demonstrates the use of the serializers.
TEST(Serializer, SimpleExample) {
  using testNamespaceA::A;
  std::string filename = "Serializer.SimpleExample.dat";
  {
    A a{42, -5};
    serialization::FileWriteSerializer writer{filename};
    writer << a;  // `writer | a` or `serialize(writer, a)` are equivalent;
  }
  {
    // `a` has been written to the file, the file has been closed, reopen it and
    // read.
    A a{};  // Uninitialized, we will read into it;
    serialization::FileReadSerializer reader{filename};
    reader >> a;
    // We have successfully restored the values.
    ASSERT_EQ(a.a, 42);
    ASSERT_EQ(a.b, -5);
  }
  ad_utility::deleteFile(filename);
}

// A example that shows how different actions can be performed for reading and
// writing;
struct T {
  int value = 42;
  mutable bool writing = false;
  bool reading = false;
  AD_SERIALIZE_FRIEND_FUNCTION(T) {
    serializer | arg.value;
    if constexpr (WriteSerializer<S>) {
      arg.writing = true;
    } else {
      arg.reading = true;
    }
  }
};

TEST(Serializer, ReadAndWriteDiffers) {
  std::string filename = "Serializer.ReadAndWriteDiffers.dat";
  {
    T t;
    serialization::FileWriteSerializer writer{filename};
    // Serialization and `if constexpr (WriteSerializer<S>)` still works when
    // the serializer is a reference.
    auto& writerRef = writer;
    writerRef << t;
    ASSERT_TRUE(t.writing);
    ASSERT_FALSE(t.reading);
  }
  {
    T t;
    serialization::FileReadSerializer reader{filename};
    auto& readerRef = reader;
    readerRef >> t;
    ASSERT_FALSE(t.writing);
    ASSERT_TRUE(t.reading);
  }
  ad_utility::deleteFile(filename);
}

// Assert that the serializers actually fulfill the `Serializer` concept.
// You should write similar tests when adding custom serializers.
TEST(Serializer, Concepts) {
  static_assert(ReadSerializer<ByteBufferReadSerializer>);
  static_assert(!WriteSerializer<ByteBufferReadSerializer>);
  static_assert(WriteSerializer<ByteBufferWriteSerializer>);
  static_assert(!ReadSerializer<ByteBufferWriteSerializer>);
  static_assert(ReadSerializer<FileReadSerializer>);
  static_assert(!WriteSerializer<FileReadSerializer>);
  static_assert(WriteSerializer<FileWriteSerializer>);
  static_assert(!ReadSerializer<FileWriteSerializer>);
  static_assert(ReadSerializer<CopyableFileReadSerializer>);
  static_assert(!WriteSerializer<CopyableFileReadSerializer>);
  {
    using Writer = ad_utility::serialization::ZstdWriteSerializer<
        ByteBufferWriteSerializer>;
    using Reader =
        ad_utility::serialization::ZstdReadSerializer<ByteBufferReadSerializer>;
    static_assert(WriteSerializer<Writer>);
    static_assert(!ReadSerializer<Writer>);
    static_assert(ReadSerializer<Reader>);
    static_assert(!WriteSerializer<Reader>);
  }
  {
    using Writer = CompressedWriteSerializer<ByteBufferWriteSerializer,
                                             decltype(dummyCompress)>;
    using Reader = CompressedReadSerializer<ByteBufferReadSerializer,
                                            decltype(dummyDecompress)>;
    static_assert(WriteSerializer<Writer>);
    static_assert(!ReadSerializer<Writer>);
    static_assert(ReadSerializer<Reader>);
    static_assert(!WriteSerializer<Reader>);
  }
}

// The following tests are mainly not for documentation but rather stress tests
// that all kinds of serializers work with all kind of builtin and user-defined
// and arbitrary nested types.
auto testWithByteBuffer = [](auto testFunction) {
  ByteBufferWriteSerializer writer;
  auto makeReaderFromWriter = [&writer]() {
    return ByteBufferReadSerializer{std::move(writer).data()};
  };
  testFunction(writer, makeReaderFromWriter);
};

auto testWithCallableSerializer = [](auto testFunction) {
  std::vector<char> buffer;
  auto write = [&buffer](const char* source, size_t numBytes) {
    buffer.insert(buffer.end(), source, source + numBytes);
  };

  WriteViaCallableSerializer writer{write};
  auto makeReaderFromWriter = [&buffer]() {
    auto read = [pos = 0ul, &buffer](char* target, size_t numBytes) mutable {
      std::copy(buffer.begin() + pos, buffer.begin() + pos + numBytes, target);
      pos += numBytes;
    };
    return ReadViaCallableSerializer{read};
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
  testWithCallableSerializer(testFunction);
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

    // Enabling cheaper unit tests when building in Debug mode
#ifdef QLEVER_RUN_EXPENSIVE_TESTS
    static constexpr int numIterations = 300'000;
#else
    static constexpr int numIterations = 300;
#endif

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
    ad_utility::HashMap<std::string, int> m;
    m["hallo"] = 42;
    m["tschüss"] = 84;

    std::string withZero = "something";
    withZero.push_back(0);
    withZero.push_back('a');
    m["withZero"] = 4321;
    writer | m;

    auto reader = makeReaderFromWriter();
    ad_utility::HashMap<std::string, int> n;
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

TEST(Serializer, Array) {
  auto testTriviallyCopyableDatatype = [](auto&& writer,
                                          auto makeReaderFromWriter) {
    std::array v_in{5, 6, 89, 42, -23948165, 0, 59309289, -42};
    std::tuple t_in{5, 3.16, 'a', 42, -23948165, 0, 59309289, -42};
    writer | v_in;
    writer | t_in;
    using namespace ad_utility::serialization;
    static_assert(TriviallySerializable<decltype(v_in)>);
    static_assert(!TriviallySerializable<decltype(t_in)>);
    static_assert(
        ad_utility::serialization::detail::tupleTriviallySerializableImpl<
            decltype(t_in)>());

    auto reader = makeReaderFromWriter();
    decltype(v_in) v_out;
    decltype(t_in) t_out;
    reader | v_out;
    reader | t_out;
    ASSERT_EQ(v_in, v_out);
    ASSERT_EQ(t_in, t_out);
  };

  auto testNonTriviallyCopyableDatatype = [](auto&& writer,
                                             auto makeReaderFromWriter) {
    using namespace std::string_literals;
    std::array v_in{"hi"s, "bye"s};
    std::tuple t_in{5,         3.16,   'a',      "bimmbamm"s,
                    -23948165, "ups"s, 59309289, -42};
    writer | v_in;
    writer | t_in;
    using namespace ad_utility::serialization;
    static_assert(!TriviallySerializable<decltype(v_in)>);
    static_assert(!TriviallySerializable<decltype(t_in)>);
    static_assert(
        !ad_utility::serialization::detail::tupleTriviallySerializableImpl<
            decltype(t_in)>());

    auto reader = makeReaderFromWriter();
    decltype(v_in) v_out;
    decltype(t_in) t_out;
    reader | v_out;
    reader | t_out;
    ASSERT_EQ(v_in, v_out);
    ASSERT_EQ(t_in, t_out);
  };

  testWithAllSerializers(testTriviallyCopyableDatatype);
  testWithAllSerializers(testNonTriviallyCopyableDatatype);
}

// Test that we can successfully write `string_view`s to a serializer and
// correctly read them as `string`s.
TEST(Serializer, StringViewToString) {
  auto testString = [](auto&& writer, auto makeReaderFromWriter) {
    std::vector<std::string_view> v = {"bim", "bam",
                                       "veryLongStringLongerThanShortString"};
    std::vector<std::string> vAsString = {
        "bim", "bam", "veryLongStringLongerThanShortString"};
    writer | v;

    auto reader = makeReaderFromWriter();
    std::vector<std::string> w;
    reader | w;
    ASSERT_EQ(vAsString, w);
  };

  testWithAllSerializers(testString);
}

TEST(Serializer, CopyAndMove) {
  auto testWithMove = [](auto&& writer, auto makeReaderFromWriter) {
    using Writer = std::decay_t<decltype(writer)>;

    // Assert that write serializers cannot be copied.
    static_assert(!requires(Writer w1, Writer w2) {
      { w1 = w2 };
    });
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
    static_assert(!requires(Reader r1, Reader r2) {
      { r1 = r2 };
    });
    static_assert(!std::constructible_from<Reader, const Reader&>);
    // Assert that moving writers consistently reads from the same resource.
    int i;
    reader | i;
    ASSERT_EQ(i, 1);
    decltype(reader) reader2{std::move(reader)};
    reader2 | i;
    ASSERT_EQ(i, 2);
    static_assert(!std::is_const_v<decltype(reader2)>);
    reader = std::move(reader2);
    reader | i;
    ASSERT_EQ(i, 3);
  };
  testWithAllSerializers(testWithMove);
}

TEST(VectorIncrementalSerializer, Serialize) {
  std::vector<int> ints{9, 7, 5, 3, 1, -1, -3, 5, 5, 6, 67498235, 0, 42};
  std::vector<std::string> strings{"alpha", "beta", "gamma", "Epsilon",
                                   "kartoffelsalat"};
  std::string filename = "vectorIncrementalTest.tmp";

  auto testIncrementalSerialization = [filename](const auto& inputVector) {
    using T = std::decay_t<decltype(inputVector)>;
    VectorIncrementalSerializer<typename T::value_type, FileWriteSerializer>
        writer{filename};
    for (const auto& element : inputVector) {
      writer.push(element);
    }
    writer.finish();
    FileReadSerializer reader{filename};
    T vectorRead;
    reader >> vectorRead;
    ASSERT_EQ(inputVector, vectorRead);
  };
  testIncrementalSerialization(ints);
  testIncrementalSerialization(strings);
  ad_utility::deleteFile(filename);
}

TEST(VectorIncrementalSerializer, SerializeInTheMiddle) {
  std::vector<int> ints{9, 7, 5, 3, 1, -1, -3, 5, 5, 6, 67498235, 0, 42};
  std::vector<std::string> strings{"alpha", "beta", "gamma", "Epsilon",
                                   "kartoffelsalat"};
  std::string filename = "vectorIncrementalTest.tmp";

  auto testIncrementalSerialization = [filename](const auto& inputVector) {
    using T = std::decay_t<decltype(inputVector)>;
    FileWriteSerializer writeSerializer{filename};
    double d = 42.42;
    writeSerializer << d;
    VectorIncrementalSerializer<typename T::value_type, FileWriteSerializer>
        writer{std::move(writeSerializer)};
    for (const auto& element : inputVector) {
      writer.push(element);
    }
    writeSerializer = FileWriteSerializer{std::move(writer).serializer()};
    d = -13.123;
    writeSerializer << d;
    writeSerializer.close();
    FileReadSerializer reader{filename};
    double doubleRead;
    reader >> doubleRead;
    ASSERT_FLOAT_EQ(doubleRead, 42.42);
    T vectorRead;
    reader >> vectorRead;
    ASSERT_EQ(inputVector, vectorRead);
    reader >> doubleRead;
    ASSERT_FLOAT_EQ(doubleRead, -13.123);
  };
  testIncrementalSerialization(ints);
  testIncrementalSerialization(strings);
  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
TEST(Serializer, serializeSpan) {
  std::vector ints{3, 4, 5, 6};
  std::vector<std::string> strings{"eins", "zwei", "drei", "vier"};
  auto intSpan = ql::span{ints}.subspan(1, 2);
  auto stringSpan = ql::span{strings}.subspan(2, 2);
  ByteBufferWriteSerializer writer;
  writer | intSpan;
  writer | stringSpan;
  writer | stringSpan;
  writer | stringSpan;
  writer | stringSpan;

  auto buffer = std::move(writer).data();
  ByteBufferReadSerializer reader(buffer);
  {
    std::vector<int> intResult;
    intResult.resize(2);
    // Read into a span of the correct size, trivially serializable
    // `value_type`.
    reader | ql::span{intResult};
    EXPECT_THAT(intResult, ::testing::ElementsAre(4, 5));
  }

  {
    std::vector<std::string> stringResult;
    stringResult.resize(2);
    // Read into a span of the correct size, nontrivially serializable
    // `value_type`.
    reader | ql::span{stringResult};
    EXPECT_THAT(stringResult, ::testing::ElementsAre("drei", "vier"));
  }

  std::vector<std::string> stringsWithWrongSize;
  // Deserialize into a `span` that doesn't have the correct size. This throws
  // an exception and skips the span.
  AD_EXPECT_THROW_WITH_MESSAGE(reader | ql::span{stringsWithWrongSize},
                               ::testing::HasSubstr("must be properly sized"));
  {
    // The writing was done via a `span`, but we now read into a `vector`.
    // This works even if the vector is not resized in advance, because the
    // vector deserialization will do the resize.
    EXPECT_NO_THROW(reader | stringsWithWrongSize);
    EXPECT_THAT(stringsWithWrongSize, ::testing::ElementsAre("drei", "vier"));
  }
}

// _____________________________________________________________________________
TEST(Serializer, serializeOptional) {
  std::optional<std::string> s = "hallo";
  std::optional<std::string> nil = std::nullopt;
  ByteBufferWriteSerializer writer;
  writer << s;
  writer << nil;
  std::optional<std::string> sExpected;
  std::optional<std::string> nilExpected = "bye";
  ByteBufferReadSerializer reader{std::move(writer).data()};
  reader >> sExpected;
  reader >> nilExpected;
  EXPECT_THAT(sExpected, ::testing::Optional(std::string("hallo")));
  EXPECT_EQ(nilExpected, std::nullopt);
}

// _____________________________________________________________________________
TEST(Serializer, serializeEnum) {
  // Enums are implicitly serializable without any additional code.
  enum E { a, b, c };
  enum struct F { d, e, f };

  ByteBufferWriteSerializer writer;
  writer << b;
  writer << F::f;

  ByteBufferReadSerializer reader(std::move(writer).data());
  E e;
  F f;
  reader | e;
  reader | f;
  EXPECT_EQ(e, E::b);
  EXPECT_EQ(f, F::f);
}

// _____________________________________________________________________________
// Tests for CompressedSerializer
// _____________________________________________________________________________

// _____________________________________________________________________________
TEST(CompressedSerializer, SimpleRoundtrip) {
  auto blockSize = ad_utility::MemorySize::bytes(3);

  ByteBufferWriteSerializer bufferWriter;
  CompressedWriteSerializer writer{std::move(bufferWriter), dummyCompress,
                                   blockSize};
  int x = 42;
  double d = 3.14159;
  std::string s = "hello world";
  writer << x;
  writer << d;
  writer << s;
  auto buffer = std::move(writer).underlyingSerializer();

  ByteBufferReadSerializer bufferReader{std::move(buffer).data()};
  CompressedReadSerializer reader{std::move(bufferReader), dummyDecompress};
  int xRead;
  double dRead;
  std::string sRead;
  reader >> xRead;
  reader >> dRead;
  reader >> sRead;

  EXPECT_EQ(x, xRead);
  EXPECT_DOUBLE_EQ(d, dRead);
  EXPECT_EQ(s, sRead);
}

// _____________________________________________________________________________
TEST(CompressedSerializer, LargeDataMultipleBlocks) {
  auto blockSize = ad_utility::MemorySize::bytes(32);

  std::vector<int> original;
  for (int i = 0; i < 1000; ++i) {
    original.push_back(i * 17 - 500);
  }

  ByteBufferWriteSerializer bufferWriter;
  CompressedWriteSerializer writer{std::move(bufferWriter), dummyCompress,
                                   blockSize};
  writer << original;
  auto buffer = std::move(writer).underlyingSerializer();

  ByteBufferReadSerializer bufferReader{std::move(buffer).data()};
  CompressedReadSerializer reader{std::move(bufferReader), dummyDecompress};
  std::vector<int> read;
  reader >> read;

  EXPECT_EQ(original, read);
}

// _____________________________________________________________________________
TEST(CompressedSerializer, ExactBlockSize) {
  // Exactly 4 ints per block
  auto blockSize = ad_utility::MemorySize::bytes(sizeof(int) * 4);

  std::vector<int> original = {1, 2, 3, 4, 5, 6, 7, 8};  // Exactly 2 blocks

  ByteBufferWriteSerializer bufferWriter;
  CompressedWriteSerializer writer{std::move(bufferWriter), dummyCompress,
                                   blockSize};
  for (int i : original) {
    writer << i;
  }
  auto buffer = std::move(writer).underlyingSerializer();

  ByteBufferReadSerializer bufferReader{std::move(buffer).data()};
  CompressedReadSerializer reader{std::move(bufferReader), dummyDecompress};
  std::vector<int> read;
  for (size_t i = 0; i < original.size(); ++i) {
    int val;
    reader >> val;
    read.push_back(val);
  }

  EXPECT_EQ(original, read);
}

// _____________________________________________________________________________
TEST(CompressedSerializer, WithFileSerializer) {
  std::string filename = "CompressedSerializer.WithFileSerializer.dat";
  auto cleanup =
      absl::Cleanup{[&filename]() { ad_utility::deleteFile(filename); }};
  auto blockSize = ad_utility::MemorySize::bytes(64);

  std::vector<double> original;
  for (int i = 0; i < 100; ++i) {
    original.push_back(i * 1.5);
  }

  {
    FileWriteSerializer fileWriter{filename};
    CompressedWriteSerializer writer{std::move(fileWriter), dummyCompress,
                                     blockSize};
    writer << original;
  }

  {
    FileReadSerializer fileReader{filename};
    CompressedReadSerializer reader{std::move(fileReader), dummyDecompress};
    std::vector<double> read;
    reader >> read;
    EXPECT_EQ(original, read);
  }
}

// _____________________________________________________________________________
TEST(ZstdSerializer, RoundtripWithByteBuffer) {
  using ad_utility::serialization::ZstdReadSerializer;
  using ad_utility::serialization::ZstdWriteSerializer;

  auto blockSize =
      ad_utility::MemorySize::kilobytes(1);  // Small block for testing

  std::vector<int> original;
  for (int i = 0; i < 100'000; ++i) {
    original.push_back(i * 17 - 500);
  }

  ByteBufferWriteSerializer bufferWriter;
  ZstdWriteSerializer writer{std::move(bufferWriter), blockSize};
  writer << original;
  auto buffer = std::move(writer).underlyingSerializer();

  ByteBufferReadSerializer bufferReader{std::move(buffer).data()};
  ZstdReadSerializer reader{std::move(bufferReader)};
  std::vector<int> read;
  reader >> read;

  EXPECT_EQ(original, read);
}

// _____________________________________________________________________________
TEST(ZstdSerializer, RoundtripWithFileSerializer) {
  using ad_utility::serialization::ZstdReadSerializer;
  using ad_utility::serialization::ZstdWriteSerializer;

  std::string filename = "ZstdSerializer.RoundtripWithFileSerializer.dat";
  auto cleanup =
      absl::Cleanup{[&filename]() { ad_utility::deleteFile(filename); }};
  auto blockSize = ad_utility::MemorySize::kilobytes(1);

  std::vector<std::string> original = {"alpha", "beta", "gamma", "delta",
                                       "epsilon"};
  for (int i = 0; i < 1000; ++i) {
    original.push_back("string_number_" + std::to_string(i));
  }

  {
    FileWriteSerializer fileWriter{filename};
    ZstdWriteSerializer writer{std::move(fileWriter), blockSize};
    writer << original;
  }

  {
    FileReadSerializer fileReader{filename};
    ZstdReadSerializer reader{std::move(fileReader)};
    std::vector<std::string> read;
    reader >> read;
    EXPECT_EQ(original, read);
  }
}
