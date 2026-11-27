// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/blobDiff/SegmentedBlobDiff.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <map>
#include <optional>

#include "util/Exception.h"
#include "util/compressionUsingZstd/SegmentedZstdFrame.h"
#include "util/compressionUsingZstd/ZstdWrapper.h"
#include "util/serializer/ByteBufferSerializer.h"

namespace qlever::blobDiff {

namespace {

using ad_utility::serialization::ByteBufferReadSerializerT;
using ad_utility::serialization::ByteBufferWriteSerializer;
using ad_utility::serialization::SerializationException;
using ad_utility::zstd::SegmentedFrame;
using ad_utility::zstd::SegmentedFrameBuilder;
using ad_utility::zstd::SegmentInfo;
using ad_utility::zstd::UninitializedBuffer;
using ErrorType = BlobDiffErrorType;

constexpr std::array<char, 8> diffFileMagic{'Q', 'L', 'V', 'R',
                                            'B', 'D', 'I', 'F'};
constexpr uint16_t segmentedDiffVersion = 3;
// The size of the header and of the entry of each segment (see the file
// format in `SegmentedBlobDiff.h`).
constexpr size_t headerSize = diffFileMagic.size() +
                              sizeof(segmentedDiffVersion) + sizeof(int32_t) +
                              sizeof(uint64_t);
constexpr size_t entrySize = sizeof(uint64_t) +
                             sizeof(ad_utility::zstd::ContentHash) +
                             sizeof(uint8_t) + 2 * sizeof(uint64_t);

// Where the content of a segment of the target comes from.
struct SegmentSource {
  enum class Kind : uint8_t { Copy = 0, Patch = 1 };
  Kind kind_ = Kind::Copy;
  // The copied segment of the base, or the reference of a patch.
  uint64_t baseSegment_ = noReference;
  uint64_t payloadSize_ = 0;
};

// The serialization of a `SegmentSource`. Throw a `SerializationException` if
// the kind is invalid.
AD_SERIALIZE_FUNCTION(SegmentSource) {
  auto kind = static_cast<uint8_t>(arg.kind_);
  serializer | kind;
  if constexpr (ad_utility::serialization::ReadSerializer<S>) {
    if (kind > 1) {
      throw SerializationException{"Invalid kind of a segment source"};
    }
    arg.kind_ = static_cast<SegmentSource::Kind>(kind);
  }
  serializer | arg.baseSegment_;
  serializer | arg.payloadSize_;
}

// The parsed content of a segmented diff file.
struct ParsedDiff {
  int32_t compressionLevel_ = 0;
  std::vector<SegmentInfo> segments_;
  std::vector<SegmentSource> sources_;
  // The payload of each segment (empty for copies).
  std::vector<ql::span<const char>> payloads_;
  // The total size of the target.
  uint64_t targetSize_ = 0;
};
using ParsedDiffOrError = std::variant<ParsedDiff, BlobDiffError>;

// Create a `BlobDiffError` of the given `type` with a message that is the
// concatenation of the `messageParts`.
template <typename... Parts>
BlobDiffError makeError(BlobDiffErrorType type, const Parts&... messageParts) {
  return BlobDiffError{type, absl::StrCat(messageParts...)};
}

// Parse and validate the `diffFile` (see the file format). Do not throw for
// any input.
ParsedDiffOrError parseDiff(ql::span<const char> diffFile,
                            ad_utility::MemorySize maxTargetSize) {
  if (diffFile.size() < headerSize) {
    return makeError(ErrorType::invalidMagicBytes,
                     "The given file is too small to be a blob diff file");
  }
  if (!isSegmentedBlobDiff(diffFile)) {
    return makeError(ErrorType::invalidVersion,
                     "The given file is not a segmented blob diff file");
  }
  ByteBufferReadSerializerT reader{
      diffFile.subspan(diffFileMagic.size() + sizeof(segmentedDiffVersion))};
  ParsedDiff diff;
  uint64_t numSegments = 0;
  reader >> diff.compressionLevel_;
  reader >> numSegments;
  // Check the number before anything is allocated (it might be huge).
  if (numSegments > (diffFile.size() - headerSize) / entrySize) {
    return makeError(ErrorType::invalidPayloadSize,
                     "The segmented blob diff file is truncated");
  }
  diff.segments_.resize(numSegments);
  diff.sources_.resize(numSegments);
  try {
    for (size_t i = 0; i < numSegments; ++i) {
      reader >> diff.segments_[i];
      reader >> diff.sources_[i];
    }
  } catch (const SerializationException& e) {
    return makeError(ErrorType::corruptedPayload,
                     "The segmented blob diff file is corrupted: ", e.what());
  }
  // The payloads follow the entries, check that their sizes add up.
  auto payloads = diffFile.subspan(headerSize + numSegments * entrySize);
  for (size_t i = 0; i < numSegments; ++i) {
    const auto& source = diff.sources_[i];
    if (source.payloadSize_ > payloads.size() ||
        (source.kind_ == SegmentSource::Kind::Copy &&
         source.payloadSize_ != 0)) {
      return makeError(ErrorType::invalidPayloadSize,
                       "The segmented blob diff file is truncated or "
                       "corrupted");
    }
    diff.payloads_.push_back(payloads.first(source.payloadSize_));
    payloads = payloads.subspan(source.payloadSize_);
    if (diff.segments_[i].size_ > maxTargetSize.getBytes() - diff.targetSize_) {
      return makeError(ErrorType::invalidTargetSize,
                       "The target of the blob diff exceeds the maximum "
                       "target size of ",
                       maxTargetSize.asString());
    }
    diff.targetSize_ += diff.segments_[i].size_;
  }
  if (!payloads.empty()) {
    return makeError(ErrorType::invalidPayloadSize,
                     "The segmented blob diff file has trailing bytes");
  }
  return diff;
}

// Decompress the complete segmented `frame` (on the producer side, where the
// time does not matter).
UninitializedBuffer decompressAll(ql::span<const char> compressed,
                                  const SegmentedFrame& frame) {
  UninitializedBuffer result(frame.segmentBegin(frame.numSegments()));
  ZstdWrapper::decompressToBuffer(compressed.data(), compressed.size(),
                                  result.data(), result.size());
  return result;
}

// Return the content of segment `i` of the `frame`, which was decompressed to
// `content`.
ql::span<const char> segmentContent(const SegmentedFrame& frame,
                                    ql::span<const char> content, size_t i) {
  return content.subspan(frame.segmentBegin(i), frame.segment(i).size_);
}

// The decompressed units of a base frame, which are decompressed on demand
// and then kept.
class DecompressedUnits {
 private:
  const SegmentedFrame& frame_;
  std::map<size_t, UninitializedBuffer> units_;

 public:
  explicit DecompressedUnits(const SegmentedFrame& frame) : frame_{frame} {}

  // Return the content of segment `i`, or `std::nullopt` if its unit is
  // corrupted.
  std::optional<ql::span<const char>> segment(size_t i) {
    const size_t u = frame_.unitOfSegment(i);
    auto it = units_.find(u);
    if (it == units_.end()) {
      auto unit = frame_.decompressUnit(u);
      if (!unit.has_value()) {
        return std::nullopt;
      }
      it = units_.emplace(u, std::move(unit.value())).first;
    }
    const uint64_t offset = frame_.segmentBegin(i) -
                            frame_.segmentBegin(frame_.firstSegmentOfUnit(u));
    return ql::span<const char>{it->second}.subspan(offset,
                                                    frame_.segment(i).size_);
  }
};

// Return the number of segments starting at segment `i` of the target that
// can be produced by copying the unit of the `base` that contains the source
// of segment `i`, or 0 if this is not possible. This requires that the
// segments of the unit are copied in order and completely, and that the unit
// ends in the start state unless it is copied to the end of the target.
size_t numSegmentsOfCopyableUnit(const ParsedDiff& diff, size_t i,
                                 const SegmentedFrame& base) {
  const size_t first = diff.sources_[i].baseSegment_;
  const size_t u = base.unitOfSegment(first);
  const size_t numSegments = base.unit(u).numSegments_;
  const size_t numTargetSegments = diff.segments_.size();
  if (base.firstSegmentOfUnit(u) != first ||
      numSegments > numTargetSegments - i ||
      (!base.unit(u).endsInStartState_ &&
       i + numSegments != numTargetSegments)) {
    return 0;
  }
  for (size_t k = 0; k < numSegments; ++k) {
    const auto& source = diff.sources_[i + k];
    if (source.kind_ != SegmentSource::Kind::Copy ||
        source.baseSegment_ != first + k) {
      return 0;
    }
  }
  return numSegments;
}

// Return the error for a diff that does not belong to the base.
BlobDiffError wrongBaseError(std::string_view reason) {
  return makeError(ErrorType::wrongBase,
                   "The segmented blob diff does not belong to the given base "
                   "blob (",
                   reason, ")");
}

// Return the error for a base whose units cannot be decompressed.
BlobDiffError corruptedBaseError() {
  return makeError(ErrorType::baseNotDecompressible,
                   "The base blob is corrupted");
}

// Check that the sources of the `diff` refer to existing segments of the
// `base`, and that the copied segments have the expected content. Return an
// error otherwise.
std::optional<BlobDiffError> checkSources(const ParsedDiff& diff,
                                          const SegmentedFrame& base) {
  for (size_t i = 0; i < diff.segments_.size(); ++i) {
    const auto& source = diff.sources_[i];
    const bool isCopy = source.kind_ == SegmentSource::Kind::Copy;
    if ((isCopy || source.baseSegment_ != noReference) &&
        source.baseSegment_ >= base.numSegments()) {
      return wrongBaseError("a segment of the base is missing");
    }
    if (isCopy && base.segment(source.baseSegment_) != diff.segments_[i]) {
      return wrongBaseError(
          "a copied segment of the base has a different hash");
    }
  }
  return std::nullopt;
}

// Apply the parsed `diff` to the `base` (see `applySegmentedBlobDiff`), whose
// sources have already been checked by `checkSources`.
BlobOrError applyParsedDiff(const ParsedDiff& diff,
                            const SegmentedFrame& base) {
  SegmentedFrameBuilder builder{diff.compressionLevel_};
  DecompressedUnits baseUnits{base};
  // The decompressed patched segments, which have to stay alive until the
  // builder is finished.
  std::vector<UninitializedBuffer> patchedSegments;
  patchedSegments.reserve(diff.segments_.size());
  for (size_t i = 0; i < diff.segments_.size();) {
    const auto& expected = diff.segments_[i];
    const auto& source = diff.sources_[i];
    if (source.kind_ == SegmentSource::Kind::Copy) {
      // Copy a complete unit if possible, otherwise decompress the segment.
      if (size_t n = numSegmentsOfCopyableUnit(diff, i, base); n > 0) {
        builder.copyUnit(base, base.unitOfSegment(source.baseSegment_));
        i += n;
        continue;
      }
      auto content = baseUnits.segment(source.baseSegment_);
      if (!content.has_value() ||
          ad_utility::zstd::hashOfContent(content.value()) != expected.hash_) {
        return corruptedBaseError();
      }
      builder.addSegment(content.value(), expected.hash_);
      ++i;
      continue;
    }
    // A patch.
    ql::span<const char> reference;
    if (source.baseSegment_ != noReference) {
      auto content = baseUnits.segment(source.baseSegment_);
      if (!content.has_value()) {
        return corruptedBaseError();
      }
      reference = content.value();
    }
    auto& target = patchedSegments.emplace_back(expected.size_);
    auto size = ZstdWrapper::tryToDecompressWithReference(
        reference, diff.payloads_[i], target.data(), target.size());
    if (std::holds_alternative<ZstdWrapper::Error>(size) ||
        std::get<size_t>(size) != target.size() ||
        ad_utility::zstd::hashOfContent(target) != expected.hash_) {
      return makeError(ErrorType::corruptedPayload,
                       "A patched segment of the blob diff cannot be "
                       "decompressed or has the wrong hash (the diff file is "
                       "corrupted or does not belong to the base)");
    }
    builder.addSegment(target, expected.hash_);
    ++i;
  }
  return std::move(builder).finish();
}

}  // namespace

// _____________________________________________________________________________
bool isSegmentedBlobDiff(ql::span<const char> diffFile) {
  constexpr size_t prefixSize =
      diffFileMagic.size() + sizeof(segmentedDiffVersion);
  if (diffFile.size() < prefixSize) {
    return false;
  }
  ByteBufferReadSerializerT reader{diffFile.first(prefixSize)};
  std::array<char, diffFileMagic.size()> magic;
  uint16_t version;
  reader >> magic;
  reader >> version;
  return magic == diffFileMagic && version == segmentedDiffVersion;
}

// _____________________________________________________________________________
std::vector<char> computeSegmentedBlobDiff(
    ql::span<const char> compressedBase, ql::span<const char> compressedTarget,
    int targetCompressionLevel, int diffCompressionLevel) {
  auto base = SegmentedFrame::read(compressedBase);
  auto target = SegmentedFrame::read(compressedTarget);
  if (!base.has_value() || !target.has_value()) {
    throw std::runtime_error{
        "A segmented blob diff can only be computed between two segmented "
        "blobs (see `BlobSerializationConfig::writeSegments_`)"};
  }
  auto baseContent = decompressAll(compressedBase, base.value());
  auto targetContent = decompressAll(compressedTarget, target.value());

  // The first segment of the base with a given content.
  std::map<std::pair<uint64_t, ad_utility::zstd::ContentHash>, size_t>
      baseSegments;
  for (size_t i = base->numSegments(); i-- > 0;) {
    baseSegments[{base->segment(i).size_, base->segment(i).hash_}] = i;
  }
  // Return the segment of the base that has the same content as segment `i`
  // of the target. Prefer the segment that follows the previously copied
  // segment, and then the segment at the same position, such that complete
  // units of the base can be copied.
  std::optional<size_t> previousCopy;
  auto findCopy = [&](size_t i) -> std::optional<size_t> {
    const auto& segment = target->segment(i);
    for (auto candidate :
         {previousCopy.has_value() ? std::optional{previousCopy.value() + 1}
                                   : std::nullopt,
          std::optional{i}}) {
      if (candidate.has_value() && candidate.value() < base->numSegments() &&
          base->segment(candidate.value()) == segment) {
        return candidate;
      }
    }
    auto it = baseSegments.find({segment.size_, segment.hash_});
    return it == baseSegments.end() ? std::nullopt : std::optional{it->second};
  };

  ByteBufferWriteSerializer writer;
  writer << diffFileMagic;
  writer << segmentedDiffVersion;
  writer << static_cast<int32_t>(targetCompressionLevel);
  writer << static_cast<uint64_t>(target->numSegments());
  std::vector<std::vector<char>> payloads;
  for (size_t i = 0; i < target->numSegments(); ++i) {
    SegmentSource source;
    previousCopy = findCopy(i);
    if (previousCopy.has_value()) {
      source.baseSegment_ = previousCopy.value();
    } else {
      // Patch the segment with the segment of the base at the same position
      // as the reference.
      source.kind_ = SegmentSource::Kind::Patch;
      ql::span<const char> reference;
      if (i < base->numSegments()) {
        source.baseSegment_ = i;
        reference = segmentContent(base.value(), baseContent, i);
      }
      payloads.push_back(ZstdWrapper::compressWithReference(
          reference, segmentContent(target.value(), targetContent, i),
          diffCompressionLevel));
      source.payloadSize_ = payloads.back().size();
    }
    writer << target->segment(i);
    writer << source;
  }
  for (const auto& payload : payloads) {
    writer.serializeBytes(payload.data(), payload.size());
  }
  return std::move(writer).data();
}

// _____________________________________________________________________________
BlobOrError applySegmentedBlobDiff(ql::span<const char> compressedBase,
                                   ql::span<const char> diffFile,
                                   ad_utility::MemorySize maxTargetSize) {
  auto diff = parseDiff(diffFile, maxTargetSize);
  if (auto* error = std::get_if<BlobDiffError>(&diff)) {
    return std::move(*error);
  }
  auto base = SegmentedFrame::read(compressedBase);
  if (!base.has_value()) {
    return makeError(ErrorType::wrongBase,
                     "A segmented blob diff can only be applied to a "
                     "segmented base blob, but the given base blob is not "
                     "segmented (or corrupted)");
  }
  const auto& parsed = std::get<ParsedDiff>(diff);
  if (auto error = checkSources(parsed, base.value())) {
    return std::move(error.value());
  }
  try {
    return applyParsedDiff(parsed, base.value());
  } catch (const std::runtime_error& error) {
    // Thrown by the `SegmentedFrameBuilder` if a unit of the base cannot be
    // decompressed.
    return makeError(ErrorType::baseNotDecompressible,
                     "The base blob is corrupted: ", error.what());
  }
}

// _____________________________________________________________________________
std::string describeSegmentedBlobDiff(ql::span<const char> diffFile) {
  auto parsed = parseDiff(diffFile, ad_utility::MemorySize::max());
  if (auto* error = std::get_if<BlobDiffError>(&parsed)) {
    throw std::runtime_error{error->message_};
  }
  const auto& diff = std::get<ParsedDiff>(parsed);
  size_t numCopies = 0;
  uint64_t copiedBytes = 0;
  uint64_t payloadBytes = 0;
  for (size_t i = 0; i < diff.segments_.size(); ++i) {
    if (diff.sources_[i].kind_ == SegmentSource::Kind::Copy) {
      ++numCopies;
      copiedBytes += diff.segments_[i].size_;
    }
    payloadBytes += diff.payloads_[i].size();
  }
  return absl::StrFormat(
      "Segmented blob diff of %d bytes: target %d bytes (decompressed) in %d "
      "segments, %d of which (%d bytes) are copied from the base, %d are "
      "patched (payload %d bytes)",
      diffFile.size(), diff.targetSize_, diff.segments_.size(), numCopies,
      copiedBytes, diff.segments_.size() - numCopies, payloadBytes);
}

}  // namespace qlever::blobDiff
