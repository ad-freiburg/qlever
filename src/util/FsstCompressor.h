// Copyright 2024, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_FSSTCOMPRESSOR_H
#define QLEVER_FSSTCOMPRESSOR_H

#include <absl/cleanup/cleanup.h>
#include <fsst.h>

#include <memory>
#include <ranges>
#include <string>
#include <vector>

#include "util/Exception.h"
#include "util/Log.h"

// A simple C++ wrapper around the C-API of the `FSST` library. It consists of
// two types, a thredsafe `FsstDecoder` that can be used to perform
// decompression, and a single-threaded `FsstEncoder` for compression.
// TODO<joka921> There are a lot of `const_cast`s that look rather fishy because
// they cast away constness. `FSST` currently has many function parameters that
// are logically const, but are not marked as const. I have opened a PR for FSST
// to make them const, as soon as this is merged, get rid of all the
// `const_cast`s and `mutable`s in this file.
class FsstDecoder {
 private:
  mutable fsst_decoder_t decoder_;

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
    output.resize(8 * str.size());
    size_t size = fsst_decompress(
        &decoder_, str.size(),
        reinterpret_cast<unsigned char*>(const_cast<char*>(str.data())),
        output.size(), reinterpret_cast<unsigned char*>(output.data()));
    // FSST compresses at most by a factor of 8.
    AD_CORRECTNESS_CHECK(size <= output.size());
    output.resize(size);
    return output;
  }
  // Allow this type to be trivially serializable,
  friend std::true_type allowTrivialSerialization(
      std::same_as<FsstDecoder> auto, auto);
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

    std::ranges::for_each(std::views::reverse(decoders_), decompressSingle);
    return result;
  }
  // Allow this type to be trivially serializable,
  [[maybe_unused]] friend std::true_type allowTrivialSerialization(
      std::same_as<FsstRepeatedDecoder> auto, auto) {
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
    auto data =
        reinterpret_cast<unsigned char*>(const_cast<char*>(word.data()));
    size_t outputLen = 0;
    size_t numCompressed =
        fsst_compress(encoder_.get(), 1, &len, &data, output.size(),
                      reinterpret_cast<unsigned char*>(output.data()),
                      &outputLen, &dummyOutput);
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
  static BulkResult compressAll(const auto& strings) {
    return makeEncoder<true>(strings);
  }

 private:
  // The implementation of the constructor and of `compressAll`.
  template <bool alsoCompressAll = false>
  static std::conditional_t<alsoCompressAll, BulkResult, Encoder> makeEncoder(
      const auto& strings) {
    std::vector<size_t> lengths;
    std::vector<unsigned char*> pointers;
    [[maybe_unused]] size_t totalSize = 0;
    for (const auto& string : strings) {
      lengths.push_back(string.size());
      totalSize += string.size();
      pointers.push_back(
          reinterpret_cast<unsigned char*>(const_cast<char*>(string.data())));
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
            output.size(), reinterpret_cast<unsigned char*>(output.data()),
            outputLengths.data(),
            reinterpret_cast<unsigned char**>(outputPtrs.data()));
        // Typically one iteration should suffice, we repeat in a loop with
        // exponential growth of the output buffer.
        if (numCompressed == strings.size()) {
          break;
        }
        LOG(DEBUG) << "FSST compression of a block of strings made the input "
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
      return std::tuple{std::move(outputPtr), std::move(stringViews),
                        FsstDecoder(fsst_decoder(encoder))};
    }
  }
};

#endif  // QLEVER_FSSTCOMPRESSOR_H
