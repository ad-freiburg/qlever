//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/util/Random.h"
#include "../src/util/Serializer/ByteBufferSerializer.h"
#include "../src/util/Serializer/FileSerializer.h"
#include "../src/util/Serializer/SerializeHashMap.h"
#include "../src/util/Serializer/SerializeString.h"
#include "../src/util/Serializer/SerializeVector.h"
#include "../src/util/Serializer/Serializer.h"

using namespace ad_utility;
using ad_utility::serialization::ByteBufferReadSerializer;
using ad_utility::serialization::ByteBufferWriteSerializer;
using ad_utility::serialization::CopyableFileReadSerializer;
using ad_utility::serialization::FileReadSerializer;
using ad_utility::serialization::FileWriteSerializer;
using ad_utility::serialization::VectorIncrementalSerializer;

using ad_utility::serialization::ReadSerializer;
using ad_utility::serialization::Serializer;
using ad_utility::serialization::WriteSerializer;

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
  AD_SERIALIZE_FRIEND_FUNCTION(C);
};
AD_SERIALIZE_FUNCTION(C) {
  serializer | arg.a;
  serializer | arg.b;
}

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
    // We have succesfully restored the values.
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

// Test that we can succesfully write `string_view`s to a serializer and
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

  auto testIncrementalSerialization =
      [filename]<typename T>(const T& inputVector) {
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

  auto testIncrementalSerialization =
      [filename]<typename T>(const T& inputVector) {
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
