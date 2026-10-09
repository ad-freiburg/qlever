// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_RDFTYPES_PARSEDGEOMETRY_H
#define QLEVER_SRC_RDFTYPES_PARSEDGEOMETRY_H

#include <spatialjoin/Sweeper.h>

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "util/Serializer/Serializer.h"

namespace ad_utility {

// The version of the stored form of a `ParsedGeometry` (see
// `GeoVocabulary`). It must be increased when the serialization of `Part`
// changes, or when `libspatialjoin` changes its `sj::BoxVal` or the format of
// its geometry cache, which invalidates all indexes with parsed geometries.
constexpr uint64_t PARSED_GEOMETRY_VERSION = 1;

// A WKT literal that was parsed and preprocessed by `libspatialjoin` at index
// build time, so that a spatial join can add it to its `sj::Sweeper` without
// parsing it at query time. The `GeoVocabulary` stores this for the WKT
// literals of at least a configurable length (see the index builder option
// `--parsed-geometries-min-length`), because parsing and preprocessing a huge
// geometry (a country boundary with millions of points) takes seconds, while
// the same for an ordinary geometry takes microseconds.
//
// A literal yields one part per geometry of a `MULTI...` or
// `GEOMETRYCOLLECTION` and one part otherwise, exactly as
// `sj::WKTParserBase::parseLine` hands geometries to the sweeper. A part is
// what `sj::Sweeper::add` produces for a geometry: the two sweep events
// (`sj::BoxVal`), the sub id, and the serialized geometry for the geometry
// cache of the sweeper (`raw_`). Two things in them depend on the query and
// are set by `addToBatch`: the id of the geometry (its row in the input of the
// spatial join, which `libspatialjoin` stores in the events and inside `raw_`),
// and the padding of the bounding boxes for a `WITHIN_DIST` join. What
// `sj::Sweeper::add` does not do for a parsed geometry is to drop it early when
// it cannot match the DE-9IM filter of the query, which only costs time.
class ParsedGeometry {
 public:
  struct Part {
    sj::BoxVal boxvalIn_;
    sj::BoxVal boxvalOut_;
    size_t subid_;
    // The position of the id (its 2-byte length followed by its bytes) inside
    // `raw_`, where `raw_` holds the placeholder id `placeholderId`.
    size_t idPosition_;
    std::string raw_;

    AD_SERIALIZE_FRIEND_FUNCTION(Part) {
      ad_utility::serialization::triviallySerialize(serializer, arg.boxvalIn_);
      ad_utility::serialization::triviallySerialize(serializer, arg.boxvalOut_);
      serializer | arg.subid_;
      serializer | arg.idPosition_;
      serializer | arg.raw_;
    }
  };

 private:
  std::vector<Part> parts_;

  // The id with which the geometry is added to the sweeper when parsing. It is
  // replaced by the actual id in `addToBatch`. It has at least 8 characters,
  // because for shorter ids `sj::Sweeper::add` folds the id into the sweep
  // events ("folded" geometry types) instead of serializing the geometry.
  static constexpr std::string_view placeholderId = "A99999999";
  static_assert(placeholderId.size() >= 8);

 public:
  // Parse `wktLiteral` (with quotes and datatype, as stored in the vocabulary)
  // and preprocess it with `sweeper`, which must be configured with
  // `sweeperConfig`. Return `std::nullopt` if `libspatialjoin` yields no
  // geometry for the literal.
  static std::optional<ParsedGeometry> fromWktLiteral(
      std::string_view wktLiteral, sj::Sweeper& sweeper);

  // The sweeper configuration for `fromWktLiteral`, without callbacks and for
  // a single thread. A spatial join that wants to use the parsed geometries
  // must use a sweeper with the same geometry options (box ids, diagonal
  // boxes, no oriented bounding boxes), because the parts depend on them.
  static sj::SweeperCfg sweeperConfig();

  // Add the geometry to `batch` for `sweeper`, as the geometry with the given
  // `row` on the given `side` of the spatial join, like `sj::Sweeper::add`
  // does for a geometry parsed at query time (the batch is then handed to
  // `sj::Sweeper::addBatch`). Return the bounding box of the geometry, padded
  // for a `WITHIN_DIST` join according to the configuration of `sweeper`.
  ::util::geo::I32Box addToBatch(const sj::Sweeper& sweeper, size_t row,
                                 bool side, sj::WriteBatch& batch) const;

  const std::vector<Part>& parts() const { return parts_; }

  // Serialization to and from a byte string (for storing on disk).
  std::string toBytes() const;
  static ParsedGeometry fromBytes(std::string_view bytes);
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_RDFTYPES_PARSEDGEOMETRY_H
