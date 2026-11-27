// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "rdfTypes/ParsedGeometry.h"

#include <spatialjoin/WKTParse.h>

#include <limits>

#include "util/Exception.h"
#include "util/serializer/ByteBufferSerializer.h"
#include "util/serializer/SerializeString.h"
#include "util/serializer/SerializeVector.h"

namespace ad_utility {

namespace {

// `parseWKTGeometry` of the parser base class of `libspatialjoin` is protected.
// This minimal subclass makes it callable without starting the parser threads
// of the base class (which its derived classes start in their constructors).
struct SingleGeometryParser : public sj::WKTParserBase<sj::ParseJob> {
  explicit SingleGeometryParser(sj::GeometryStore* store)
      : sj::WKTParserBase<sj::ParseJob>(store, sj::PreparedGeomMetaCb{}, 1) {}
  void processQueue(size_t) override {}
  using sj::WKTParserBase<sj::ParseJob>::parseWKTGeometry;
};

// The bytes with which `libspatialjoin` serializes the id `id` of a geometry
// in front of the geometry (see `sj::TypedGeometryStore::writeTo`): its length
// as two bytes, followed by the id.
std::string serializedId(std::string_view id) {
  AD_CORRECTNESS_CHECK(id.size() <= std::numeric_limits<uint16_t>::max());
  uint16_t length = static_cast<uint16_t>(id.size());
  std::string result(reinterpret_cast<const char*>(&length), sizeof(length));
  result += id;
  return result;
}

// The list of `batch` for the geometries of the given `type` (see
// `sj::WriteBatch`), for the types that a parsed geometry can have.
std::vector<sj::WriteCand>& candidatesOfType(sj::WriteBatch& batch,
                                             sj::GeomType type) {
  switch (type) {
    case sj::POINT:
      return batch.points;
    case sj::LINE:
      return batch.lines;
    case sj::SIMPLE_POLYGON:
      return batch.simpleAreas;
    case sj::POLYGON:
      return batch.areas;
    default:
      AD_FAIL();
  }
}

}  // namespace

// ____________________________________________________________________________
sj::SpatialJoinCfg ParsedGeometry::spatialJoinConfig() {
  sj::SpatialJoinCfg cfg;
  cfg.numThreads = 1;
  cfg.numCacheBuckets = 1;
  cfg.useBoxIds = true;
  cfg.useDiagBox = true;
  cfg.noGeometryChecks = false;
  cfg.euclideanDist = false;
  cfg.haversineApprox = false;
  cfg.computeDE9IM = false;
  cfg.de9imFilter = ::util::geo::FANY;
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
    std::string_view wktLiteral, sj::GeometryStore& store) {
  // Parse the literal with the placeholder id, in the strict mode that the
  // spatial join uses at query time. The parser needs a null-terminated
  // string.
  std::string wkt{wktLiteral};
  sj::WriteBatch batch;
  SingleGeometryParser parser{&store};
  parser.parseWKTGeometry(wkt.c_str(), std::string{placeholderId}, 0, batch,
                          true);

  // A literal with several geometries yields one multi geometry with the
  // geometries as members, any other literal a single geometry.
  ParsedGeometry result;
  result.isMulti_ = !batch.multiGeoms.empty();
  AD_CORRECTNESS_CHECK(batch.multiGeoms.size() <= 1);
  const sj::WriteBatch& geometries =
      result.isMulti_ ? batch.multiGeoms[0].members : batch;
  AD_CORRECTNESS_CHECK(!result.isMulti_ || batch.size() == geometries.size());

  // Nothing is folded, because the placeholder id is long enough. A line with
  // two points (a "simple line") is not stored, it is parsed at query time as
  // before.
  AD_CORRECTNESS_CHECK(
      geometries.foldedPoints.empty() && geometries.foldedSimpleLines.empty() &&
      geometries.foldedBoxAreas.empty() && geometries.multiGeoms.empty());
  if (!geometries.simpleLines.empty()) {
    return std::nullopt;
  }

  // The parts in the order in which `addToBatch` puts them back into a batch,
  // each without the placeholder id in front of the serialized geometry.
  auto placeholder = serializedId(placeholderId);
  for (const auto* cands : {&geometries.points, &geometries.lines,
                            &geometries.simpleAreas, &geometries.areas}) {
    for (const auto& cand : *cands) {
      AD_CORRECTNESS_CHECK(cand.raw.starts_with(placeholder));
      result.parts_.push_back(
          {cand.meta.bbox, cand.meta.box45, cand.meta.numAnchors, cand.type,
           cand.rightPoint, cand.subid, cand.raw.substr(placeholder.size())});
    }
  }
  if (result.parts_.empty()) {
    return std::nullopt;
  }
  return result;
}

// ____________________________________________________________________________
::util::geo::I32Box ParsedGeometry::addToBatch(const sj::GeometryStore& store,
                                               size_t row,
                                               sj::WriteBatch& batch) const {
  // The id of the geometry, as the parser of the spatial join composes it.
  std::string id = std::to_string(row);
  auto idPrefix = serializedId(id);

  // The members of a multi geometry are collected in a batch of their own.
  sj::WriteBatch members;
  sj::WriteBatch& target = isMulti_ ? members : batch;
  ::util::geo::I32Box boundingBox;
  for (const auto& part : parts_) {
    sj::WriteCand cand;
    cand.raw = idPrefix + part.rawWithoutId_;
    cand.gid = id;
    cand.subid = part.subid_;
    cand.meta.bbox = part.boundingBox_;
    cand.meta.box45 = part.diagonalBoundingBox_;
    cand.meta.numAnchors = part.numAnchors_;
    cand.type = part.type_;
    cand.rightPoint = part.rightPoint_;
    boundingBox = ::util::geo::extendBox(
        store.getPaddedBoundingBox(part.boundingBox_), boundingBox);
    candidatesOfType(target, part.type_).push_back(std::move(cand));
  }
  if (isMulti_) {
    store.addMulti(std::move(members), boundingBox, id, batch);
  }
  return boundingBox;
}

// ____________________________________________________________________________
std::string ParsedGeometry::toBytes() const {
  ad_utility::serialization::ByteBufferWriteSerializer writer;
  writer | isMulti_;
  writer | parts_;
  const auto& data = writer.data();
  return std::string(data.begin(), data.end());
}

// ____________________________________________________________________________
ParsedGeometry ParsedGeometry::fromBytes(std::string_view bytes) {
  ad_utility::serialization::ByteBufferReadSerializer reader{
      std::vector<char>(bytes.begin(), bytes.end())};
  ParsedGeometry result;
  reader | result.isMulti_;
  reader | result.parts_;
  return result;
}

}  // namespace ad_utility
