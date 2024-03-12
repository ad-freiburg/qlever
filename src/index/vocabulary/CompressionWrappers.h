//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <concepts>

#include "index/PrefixHeuristic.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "util/FsstCompressor.h"

namespace ad_utility::vocabulary {

// A helper concept for the compression wrappers below.
// A `BulkResultForDecoder` is a tuple of 3 elements, the first of which is an
// implementation detail (e.g. a buffer that stores the data for the second
// argument ), the second of which is a `vector<string_view>` or
// `vector<string>` that stores compressed strings and the third of which is a
// `Decoder`.
template <typename T, typename Decoder>
concept BulkResultForDecoder = requires(T t) {
  requires(std::tuple_size_v<T> == 3);
  {
    std::get<1>(t)
  } -> ad_utility::SimilarToAny<std::vector<std::string_view>,
                                std::vector<std::string>>;
  { std::get<2>(t) } -> ad_utility::SimilarTo<Decoder>;
};

// A concept for the `CompressionWrappers` that can be passed as template
// arguments to the compressed vocabulary to specify the compression algorithm.
template <typename T>
concept CompressionWrapper = requires(const T& t) {
  typename T::Decoder;
  // Return the number of decoders that are stored.
  { t.numDecoders() } -> std::same_as<size_t>;
  // Decompress the given string, use the Decoder specified by the second
  // argument.
  { t.decompress(std::string_view{}, size_t{0}) } -> std::same_as<std::string>;
  // Compress all the strings and return the strings together with a `Decoder`
  // that can be used to decompress the strings again.
  {
    T::compressAll(std::vector<std::string>{})
  } -> BulkResultForDecoder<typename T::Decoder>;
  requires(std::constructible_from<T, std::vector<typename T::Decoder>>);
};

namespace detail {
// A class that holds a `vector<DecoderT>` and implements the
// `decompress(string_view, index)` method by forwarding this call to
// `decoders_[index].decompress(string_view)`. It is used as a building block
// for types that are supposed to fulfill the `CompressionWrapper` concept
// above.
template <typename DecoderT>
struct DecoderMultiplexer {
  using Decoder = DecoderT;
  using Decoders = std::vector<Decoder>;
  using Strings = std::vector<std::string>;

 private:
  std::vector<Decoder> decoders_;

 public:
  DecoderMultiplexer() = default;
  explicit DecoderMultiplexer(Decoders decoders)
      : decoders_{std::move(decoders)} {}
  std::string decompress(std::string_view compressed,
                         size_t decoderIndex) const {
    return decoders_.at(decoderIndex).decompress(compressed);
  }
  size_t numDecoders() const { return decoders_.size(); }
};
}  // namespace detail

// A compression wrapper that applies the FSST compression algorithm.
struct FsstCompressionWrapper : detail::DecoderMultiplexer<FsstDecoder> {
  using Base = detail::DecoderMultiplexer<FsstDecoder>;
  using Base::Base;
  static FsstEncoder::BulkResult compressAll(const Strings& strings) {
    return FsstEncoder::compressAll(strings);
  }
};
static_assert(CompressionWrapper<FsstCompressionWrapper>);

// A compression wrapper that applies the FSST compression algorithm twice.
struct FsstSquaredCompressionWrapper
    : detail::DecoderMultiplexer<FsstRepeatedDecoder<2>> {
  using Base = detail::DecoderMultiplexer<FsstRepeatedDecoder<2>>;
  using Base::Base;
  using BulkResult =
      std::tuple<std::shared_ptr<std::string>, std::vector<std::string_view>,
                 FsstRepeatedDecoder<2>>;
  static BulkResult compressAll(const Strings& strings) {
    auto [buffer, views, decoder1] = FsstEncoder::compressAll(strings);
    auto [buffer2, views2, decoder2] = FsstEncoder::compressAll(views);
    return {std::move(buffer2), std::move(views2),
            FsstRepeatedDecoder{std::array{decoder1, decoder2}}};
  }
};
static_assert(CompressionWrapper<FsstSquaredCompressionWrapper>);

// A compression wrapper that compresses common prefixes using the greedy
// algorithm from `PrefixHeuristics.h`.
struct PrefixCompressionWrapper : detail::DecoderMultiplexer<PrefixCompressor> {
  using Base = detail::DecoderMultiplexer<PrefixCompressor>;
  using Base::Base;
  using BulkResult = std::tuple<bool, std::vector<std::string>, Decoder>;

  static BulkResult compressAll(const Strings& strings) {
    PrefixCompressor compressor;
    auto stringsCopy = strings;
    std::ranges::sort(stringsCopy);
    auto prefixes =
        calculatePrefixes(stringsCopy, NUM_COMPRESSION_PREFIXES, 1, true);
    compressor.buildCodebook(prefixes);
    Strings compressedStrings;
    for (const auto& string : strings) {
      compressedStrings.push_back(compressor.compress(string));
    }
    return {true, std::move(compressedStrings), std::move(compressor)};
  }
};
static_assert(CompressionWrapper<PrefixCompressionWrapper>);
}  // namespace ad_utility::vocabulary
