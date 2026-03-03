// Copyright 2024, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_FSSTCOMPRESSOR_H
#define QLEVER_FSSTCOMPRESSOR_H

#include <absl/cleanup/cleanup.h>
#include <fsst.h>

#include <memory>
#include <string>
#include <vector>

#include "util/Concepts.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/TypeTraits.h"

namespace detail {
// A helper function to cast `char*` to `unsigned char*` and `const char*` to
// `const unsigned char*` which is used below because FSST always works on
// unsigned character types. Note that this is one of the few cases where a
// `reinterpret_cast` is safe.
struct CastToUnsignedPtr {
  CPP_template(typename T)(
      requires ad_utility::SameAsAny<T, char*, const char*>) auto
  operator()(T ptr) const {
    using Res = std::conditional_t<ql::concepts::same_as<T, const char*>,
                                   const unsigned char*, unsigned char*>;
    return reinterpret_cast<Res>(ptr);
  };
};
constexpr CastToUnsignedPtr castToUnsignedPtr{};
}  // namespace detail

// A simple C++ wrapper around the C-API of the `FSST` library. It consists of
// two types, a thredsafe `FsstDecoder` that can be used to perform
// decompression, and a single-threaded `FsstEncoder` for compression.
class FsstDecoder {
 private:
  fsst_decoder_t decoder_;

 public:
  // The default constructor does lead to an invalid decoder, but is required
  // for the serialization module. Don't use it.
  FsstDecoder() = default;

  // Construct from the internal `fsst_decoder_t`. Note that the typical way to
  // obtain an `FsstDecoder` is by first creating a `FsstEncoder` and calling
  // `getDecoder()` on that encoder.
  explicit FsstDecoder(const fsst_decoder_t& decoder) : decoder_{decoder} {}

  // Decompress a  single string.
  std::string decompress(std::string_view str) const {
    std::string output;
    auto cast = detail::castToUnsignedPtr;
    output.resize(8 * str.size());
    size_t size = fsst_decompress(&decoder_, str.size(), cast(str.data()),
                                  output.size(), cast(output.data()));
    // FSST compresses at most by a factor of 8.
    AD_CORRECTNESS_CHECK(size <= output.size());
    output.resize(size);
    return output;
  }
  // Allow this type to be trivially serializable,
  CPP_template(typename T, typename U)(
      requires ql::concepts::same_as<T, FsstDecoder>) friend std::true_type
      allowTrivialSerialization(T, U&&) {
    return {};
  }
};

// A sequence of `N` `FsstDecoder` s that are chained in inverted order (the
// last one first) when decompressing a string. The inverted order is chosen,
// because it is the correct way to decompress a string that was compressed by
// the N corresponding encoders in the "normal" order (first encoder first).
template <size_t N = 2>
class FsstRepeatedDecoder {
 public:
  using Decoders = std::array<FsstDecoder, N>;

 private:
  Decoders decoders_;

 public:
  // The default constructor does lead to an invalid decoder, but is required
  // for the serialization module. Don't use it.
  FsstRepeatedDecoder() = default;

  // Construct from the internal `fsst_decoder_t`. Note that the typical way to
  // obtain an `FsstDecoder` is by first creating a `FsstEncoder` and calling
  // `getDecoder()` on that encoder.
  explicit FsstRepeatedDecoder(Decoders decoders) : decoders_{decoders} {}

  // Decompress a  single string.
  std::string decompress(std::string_view str) const {
    std::string result;
    std::string_view nextInput = str;
    auto decompressSingle = [&result, &nextInput](const FsstDecoder& decoder) {
      result = decoder.decompress(nextInput);
      nextInput = result;
    };

    ql::ranges::for_each(ql::views::reverse(decoders_), decompressSingle);
    return result;
  }
  // Allow this type to be trivially serializable,
  CPP_template_2(typename T, typename U)(
      requires ql::concepts::same_as<T, FsstRepeatedDecoder>)
      [[maybe_unused]] friend std::true_type allowTrivialSerialization(T, U) {
    return {};
  }
};

// The encoder class.
class FsstEncoder {
 private:
  // The encoder state of FSST is rather complex and managed via a pointer
  // indirection. We manage this using a `unique_ptr` with a custom deleter.
  struct Deleter {
    void operator()(fsst_encoder_t* ptr) const { fsst_destroy(ptr); }
  };
  using Encoder = std::unique_ptr<fsst_encoder_t, Deleter>;
  Encoder encoder_;
  static constexpr auto cast = detail::castToUnsignedPtr;

 public:
  // Create an `FsstEncoder`. The given `strings` are used to create the
  // codebook.
  explicit FsstEncoder(const std::vector<std::string>& strings)
      : encoder_{makeEncoder(strings)} {}

  // Compress a single string.
  std::string compress(std::string_view word) {
    size_t len = word.size();
    std::string output;
    output.resize(7 + 2 * len);
    unsigned char* dummyOutput;
    auto data = cast(word.data());
    size_t outputLen = 0;
    size_t numCompressed =
        fsst_compress(encoder_.get(), 1, &len, &data, output.size(),
                      cast(output.data()), &outputLen, &dummyOutput);
    AD_CORRECTNESS_CHECK(numCompressed == 1);
    output.resize(outputLen);
    return output;
  }

  // Return a decoder, that can be used to decompress strings that have been
  // compressed by this encoder.
  FsstDecoder makeDecoder() const {
    return FsstDecoder{fsst_decoder(encoder_.get())};
  }

  // Interface for the case that all the strings that shall ever be compressed
  // using the same codebook shall also contribute to that codebook. Build a
  // codebook from the `strings`, and then use that codebook to compress each of
  // the `strings`. The result consists of a large `std::string` that contains
  // all the compressed strings concatenated, a `vector<string_view` that points
  // to the compressed strings, and a decoder that can be used to decompress the
  // strings again.
  using BulkResult = std::tuple<std::shared_ptr<std::string>,
                                std::vector<std::string_view>, FsstDecoder>;
  template <typename T>
  static BulkResult compressAll(const T& strings) {
    return makeEncoder<true>(strings);
  }

 private:
  // The implementation of the constructor and of `compressAll`.
  template <bool alsoCompressAll = false, typename Strings>
  static std::conditional_t<alsoCompressAll, BulkResult, Encoder> makeEncoder(
      const Strings& strings) {
    std::vector<size_t> lengths;
    std::vector<const unsigned char*> pointers;
    [[maybe_unused]] size_t totalSize = 0;
    for (const auto& string : strings) {
      lengths.push_back(string.size());
      totalSize += string.size();
      pointers.push_back(cast(string.data()));
    }
    auto encoder =
        fsst_create(strings.size(), lengths.data(), pointers.data(), 0);
    if constexpr (!alsoCompressAll) {
      return Encoder{encoder, Deleter{}};
    } else {
      absl::Cleanup cleanup{[&encoder]() { fsst_destroy(encoder); }};
      auto outputPtr = std::make_unique<std::string>();
      std::string& output = *outputPtr;
      output.resize(totalSize);
      std::vector<char*> outputPtrs;
      outputPtrs.resize(strings.size());
      std::vector<size_t> outputLengths;
      outputLengths.resize(strings.size());
      while (true) {
        size_t numCompressed = fsst_compress(
            encoder, strings.size(), lengths.data(), pointers.data(),
            output.size(), cast(output.data()), outputLengths.data(),
            reinterpret_cast<unsigned char**>(outputPtrs.data()));
        // Typically one iteration should suffice, we repeat in a loop with
        // exponential growth of the output buffer.
        if (numCompressed == strings.size()) {
          break;
        }
        AD_LOG_DEBUG << "FSST compression of a block of strings made the input "
                        "larger instead of smaller"
                     << std::endl;
        output.resize(2 * output.size());
      }
      // Convert the result pointers to `string_views` for easier handling.
      std::vector<std::string_view> stringViews;
      stringViews.reserve(strings.size());
      for (size_t i = 0; i < strings.size(); ++i) {
        stringViews.emplace_back(outputPtrs.at(i), outputLengths.at(i));
      }
      return BulkResult{std::move(outputPtr), std::move(stringViews),
                        FsstDecoder(fsst_decoder(encoder))};
    }
  }
};

#endif  // QLEVER_FSSTCOMPRESSOR_H
