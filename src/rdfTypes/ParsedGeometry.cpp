// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "rdfTypes/ParsedGeometry.h"

#include <absl/strings/str_cat.h>
#include <spatialjoin/WKTParse.h>

#include <limits>

#include "util/Exception.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"

namespace ad_utility {

namespace {

// `parseLine` of the parser base class of `libspatialjoin` is protected. This
// minimal subclass makes it callable without starting the parser threads of
// the base class (which its derived classes start in their constructors).
struct SingleLineParser : public sj::WKTParserBase<sj::ParseJob> {
  explicit SingleLineParser(sj::Sweeper* sweeper)
      : sj::WKTParserBase<sj::ParseJob>(sweeper, 1) {}
  void processQueue(size_t) override {}
  using sj::WKTParserBase<sj::ParseJob>::parseLine;
};

// The bytes with which `libspatialjoin` serializes the id `id` of a geometry
// into the geometry cache (see `sj::GeometryCache::writeTo`): its length as
// two bytes, followed by the id.
std::string serializedId(std::string_view id) {
  AD_CORRECTNESS_CHECK(id.size() <= std::numeric_limits<uint16_t>::max());
  uint16_t length = static_cast<uint16_t>(id.size());
  std::string result(reinterpret_cast<const char*>(&length), sizeof(length));
  result += id;
  return result;
}

}  // namespace

// ____________________________________________________________________________
sj::SweeperCfg ParsedGeometry::sweeperConfig() {
  sj::SweeperCfg cfg;
  cfg.numThreads = 1;
  cfg.numCacheThreads = 1;
  cfg.geomCacheMaxSize = 0;
  cfg.geomCacheMaxNumElements = 0;
  cfg.useBoxIds = true;
  cfg.useArea = true;
  cfg.useOBB = false;
  cfg.useDiagBox = true;
  cfg.useFastSweepSkip = true;
  cfg.noGeometryChecks = false;
  cfg.euclideanDist = false;
  cfg.haversineApprox = false;
  cfg.computeDE9IM = false;
  cfg.de9imFilter = ::util::geo::FANY;
  // Never let `libspatialjoin` fall back to a self-join when it considers one
  // side to be empty; QLever's callbacks rely on the first geometry of each
  // result pair coming from the left side and the second one from the right
  // side (see #3068).
  cfg.forceTwoSided = true;
  // This has to be set to a value < 0 to disable the `WITHIN_DIST`
  // calculation in `libspatialjoin`.
  cfg.withinDist = -1;
  cfg.writeRelCb = {};
  cfg.logCb = {};
  cfg.statsCb = {};
  cfg.sweepProgressCb = {};
  cfg.sweepCancellationCb = {};
  return cfg;
}

// ____________________________________________________________________________
std::optional<ParsedGeometry> ParsedGeometry::fromWktLiteral(
    std::string_view wktLiteral, sj::Sweeper& sweeper) {
  // Parse the literal with the placeholder id (`parseLine` composes the id
  // from the line number given to it and the side, see `sj::Sweeper::add`).
  // It needs a null-terminated string.
  static constexpr size_t placeholderLine = 99'999'999;
  static_assert(placeholderId == "A99999999");
  std::string wkt{wktLiteral};
  sj::WriteBatch batch;
  SingleLineParser parser{&sweeper};
  parser.parseLine(wkt.data(), wkt.size(), placeholderLine, 0, batch, false,
                   false);

  // The parts in the order in which `addToBatch` puts them back into a batch.
  // Nothing is folded (the placeholder id is long enough) or a reference.
  AD_CORRECTNESS_CHECK(batch.foldedPoints.empty() &&
                       batch.foldedSimpleLines.empty() &&
                       batch.foldedBoxAreas.empty() && batch.refs.empty());
  ParsedGeometry result;
  auto placeholder = serializedId(placeholderId);
  for (const auto* cands : {&batch.points, &batch.simpleLines, &batch.lines,
                            &batch.simpleAreas, &batch.areas}) {
    for (const auto& cand : *cands) {
      // Find the placeholder id inside the serialized geometry. A second
      // occurrence (in the coordinates, in theory) would make the position
      // ambiguous, then the geometry is parsed at query time as before.
      size_t idPosition = cand.raw.find(placeholder);
      if (idPosition == std::string::npos ||
          cand.raw.find(placeholder, idPosition + 1) != std::string::npos) {
        return std::nullopt;
      }
      result.parts_.push_back(
          {cand.boxvalIn, cand.boxvalOut, cand.subid, idPosition, cand.raw});
    }
  }
  if (result.parts_.empty()) {
    return std::nullopt;
  }
  return result;
}

// ____________________________________________________________________________
::util::geo::I32Box ParsedGeometry::addToBatch(const sj::Sweeper& sweeper,
                                               size_t row, bool side,
                                               sj::WriteBatch& batch) const {
  // The id of the geometry, composed like in `sj::Sweeper::add`.
  std::string id = absl::StrCat(side ? "B" : "A", row);
  auto placeholder = serializedId(placeholderId);
  auto actual = serializedId(id);

  ::util::geo::I32Box boundingBox;
  for (const auto& part : parts_) {
    sj::WriteCand cand{part.raw_, id, part.boxvalIn_, part.boxvalOut_,
                       part.subid_};
    cand.raw.replace(part.idPosition_, placeholder.size(), actual);
    auto& in = cand.boxvalIn;
    auto& out = cand.boxvalOut;
    in.side = side;
    out.side = side;

    // Pad the bounding box and the diagonal bounding box for a `WITHIN_DIST`
    // join (a no-op otherwise), like `sj::Sweeper::add` does for a geometry
    // parsed at query time. The stored events hold the unpadded boxes.
    ::util::geo::I32Box rawBox{{in.val, in.loY}, {out.val, in.upY}};
    auto box = sweeper.getPaddedBoundingBox(rawBox);
    auto box45 = sweeper.getPaddedBoundingBox(in.b45, rawBox);
    in.loY = out.loY = box.getLowerLeft().getY();
    in.upY = out.upY = box.getUpperRight().getY();
    in.val = box.getLowerLeft().getX();
    out.val = box.getUpperRight().getX();
    in.b45 = out.b45 = box45;
    boundingBox = ::util::geo::extendBox(box, boundingBox);

    // The list of the batch for the geometry type (see the `WriteBatch`).
    auto& cands = [&batch, type = in.type]() -> std::vector<sj::WriteCand>& {
      switch (type) {
        case sj::POINT:
          return batch.points;
        case sj::SIMPLE_LINE:
          return batch.simpleLines;
        case sj::LINE:
          return batch.lines;
        case sj::SIMPLE_POLYGON:
          return batch.simpleAreas;
        case sj::POLYGON:
          return batch.areas;
        default:
          AD_FAIL();
      }
    }();
    cands.push_back(std::move(cand));
  }
  return boundingBox;
}

// ____________________________________________________________________________
std::string ParsedGeometry::toBytes() const {
  ad_utility::serialization::ByteBufferWriteSerializer writer;
  writer | parts_;
  const auto& data = writer.data();
  return std::string(data.begin(), data.end());
}

// ____________________________________________________________________________
ParsedGeometry ParsedGeometry::fromBytes(std::string_view bytes) {
  ad_utility::serialization::ByteBufferReadSerializer reader{
      std::vector<char>(bytes.begin(), bytes.end())};
  ParsedGeometry result;
  reader | result.parts_;
  return result;
}

}  // namespace ad_utility
