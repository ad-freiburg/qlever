// Copyright 2024, 2026, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>
//         Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR

#ifndef QLEVER_FSSTCOMPRESSOR_H
#define QLEVER_FSSTCOMPRESSOR_H

#include <absl/cleanup/cleanup.h>
#include <fsst.h>

#include <array>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "backports/span.h"
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
  }
};
constexpr CastToUnsignedPtr castToUnsignedPtr{};

// Allocate `bound` bytes, run `decode` into that buffer, and shrink to the
// number of bytes written.
CPP_template(typename Decode)(
    requires ql::concepts::invocable<Decode, ql::span<char>>) std::string
    decompressToOwnedString(size_t bound, Decode decode) {
  std::string output;
  output.resize(bound);
  output.resize(decode(ql::span<char>{output.data(), output.size()}));
  return output;
}
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

  // Use the FSST library guarantee: expansion is at most this factor.
  static constexpr size_t maxExpansionFactor = 8;

  // Return an upper bound on the decompressed size of `str`.
  [[nodiscard]] static size_t maxDecompressedSize(std::string_view str) {
    AD_CONTRACT_CHECK(str.size() <=
                      std::numeric_limits<size_t>::max() / maxExpansionFactor);
    return maxExpansionFactor * str.size();
  }

  // Decompress `str` into `out`. `out.size()` must be at least
  // `maxDecompressedSize(str)`. Return the number of bytes written.
  [[nodiscard]] size_t decompressInto(std::string_view str,
                                      ql::span<char> out) const {
    const size_t bound = maxDecompressedSize(str);
    AD_CONTRACT_CHECK(out.size() >= bound);
    if (bound == 0) {
      return 0;
    }
    auto cast = detail::castToUnsignedPtr;
    size_t size = fsst_decompress(&decoder_, str.size(), cast(str.data()),
                                  bound, cast(out.data()));
    AD_CORRECTNESS_CHECK(size <= bound);
    return size;
  }

  // Decompress a single string. Callers that already own an output buffer
  // should use `decompressInto` instead.
  std::string decompress(std::string_view str) const {
    return detail::decompressToOwnedString(
        maxDecompressedSize(str),
        [this, str](ql::span<char> out) { return decompressInto(str, out); });
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
  static_assert(N >= 1, "FsstRepeatedDecoder needs at least one stage");

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

  // Return an upper bound on the size after all `N` decoding stages.
  [[nodiscard]] static size_t maxDecompressedSize(std::string_view str) {
    size_t bound = str.size();
    for (size_t stage = 0; stage < N; ++stage) {
      AD_CONTRACT_CHECK(bound <= std::numeric_limits<size_t>::max() /
                                     FsstDecoder::maxExpansionFactor);
      bound *= FsstDecoder::maxExpansionFactor;
    }
    return bound;
  }

  // Decompress `str` into `out`. `out.size()` must be at least
  // `maxDecompressedSize(str)`. For `N >= 2`, grow `scratch` to `out.size()`
  // if it is smaller, then ping-pong stages between `out` and `scratch` so
  // the last stage always writes `out`. Return the number of bytes written.
  [[nodiscard]] size_t decompressInto(std::string_view str, ql::span<char> out,
                                      std::string& scratch) const {
    AD_CONTRACT_CHECK(out.size() >= maxDecompressedSize(str));
    if constexpr (N >= 2) {
      if (scratch.size() < out.size()) {
        scratch.resize(out.size());
      }
    }
    std::array<ql::span<char>, 2> buffers{
        out, ql::span<char>{scratch.data(), scratch.size()}};
    // For even `N`, write the first stage to `scratch` and the last to `out`.
    // For odd `N`, write the first and last stages to `out`.
    size_t dest = (N % 2 == 0) ? 1 : 0;
    std::string_view input = str;
    size_t n = 0;
    for (size_t stage = 0; stage < N; ++stage) {
      n = decoders_[N - 1 - stage].decompressInto(input, buffers[dest]);
      input = std::string_view{buffers[dest].data(), n};
      dest ^= 1;
    }
    return n;
  }

  // Decompress a single string. Callers that already own an output buffer
  // should use `decompressInto` instead.
  std::string decompress(std::string_view str) const {
    std::string scratch;
    return detail::decompressToOwnedString(
        maxDecompressedSize(str),
        [&](ql::span<char> out) { return decompressInto(str, out, scratch); });
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
