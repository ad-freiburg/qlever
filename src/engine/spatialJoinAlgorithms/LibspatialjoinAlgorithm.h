// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2025        Patrick Brosi <brosi@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_LIBSPATIALJOINALGORITHM_H
#define QLEVER_SRC_ENGINE_LIBSPATIALJOINALGORITHM_H

#include <spatialjoin/GeometryStore.h>
#include <util/geo/Geo.h>

#include <string>

#include "engine/SpatialJoinParser.h"
#include "engine/spatialJoinAlgorithms/SpatialJoinAlgorithmBase.h"
#include "index/Index.h"
#include "util/memorySize/MemorySize.h"

// Spatial join for all `SpatialJoinType`s (`INTERSECTS`, `CONTAINS`, `WITHIN`,
// `WITHIN_DIST`, `DE9IM`, ...) and all geometry types, backed by the
// `libspatialjoin` sweepline algorithm.
class LibspatialjoinAlgorithm : public SpatialJoinAlgorithmBase {
 public:
  // In addition to the parameters shared by all algorithms, this is the only
  // algorithm that also needs the bounding-box prefilter columns (used for
  // prefiltering geometries before parsing, see `parse`).
  LibspatialjoinAlgorithm(QueryExecutionContext* qec,
                          PreparedSpatialJoinParams params,
                          SpatialJoinConfiguration config,
                          std::optional<SpatialJoin*> spatialJoin,
                          SpatialJoinBoundingBoxColumns boundingBoxColsLeft,
                          SpatialJoinBoundingBoxColumns boundingBoxColsRight)
      : SpatialJoinAlgorithmBase(qec, std::move(params), std::move(config),
                                 spatialJoin),
        boundingBoxColsLeft_{std::move(boundingBoxColsLeft)},
        boundingBoxColsRight_{std::move(boundingBoxColsRight)} {}

  Result run() override;

  // Parse the WKT geometries from the given `column` in `idTable` into the
  // `store_` of the `target` and add them to its `events_` (the
  // `sj::SweepEventList` of one side of the spatial join), using up to
  // `numThreads` threads. A geometry from the persisted store of the index (if
  // the `target` has a `persistedStoreId_`) is added to the `events_` without
  // parsing, once for all of its rows. Return the bounding box of all added
  // geometries and the counters described in `ParseMetadata`. This function is
  // only `public` for testing purposes and should otherwise not be used outside
  // of this class.
  //
  // NOTE 1: If a `prefilterBox` is given, geometries whose precomputed
  // bounding box (from the `GeoVocabulary` or the bounding box columns) does
  // not intersect it are neither parsed nor added, unless the box is too large
  // (see `maxAreaPrefilterBox`). The same holds for `GeoPoint`s and geometries
  // from the persisted store outside of it, regardless of the size of the box.
  // All other geometries are parsed (see the filter box of `store_` in
  // `run()`).
  //
  // NOTE 2: For a proximity search, the bounding box of each geometry is
  // padded by half the distance (see `sj::GeometryStore::add`).
  struct ParseInput {
    const IdTableView<0>* idTable_;
    ColumnIndex geomsCol_;
    SpatialJoinBoundingBoxColumns boundingBoxCols_;
  };
  struct ParseMetadata {
    // Aggregated bounding box of all parsed geometries
    util::geo::I32Box aggBoundingBox_;
    // Number of geometries that were actually parsed excluding prefiltered ones
    size_t numGeomsParsed_;
    // Number of geometries dropped by prefilter
    size_t numGeomsDropped_;
    // Number of parsed geometries skipped because they are invalid WKT
    size_t numGeomsInvalid_;
    // Actual number of threads used (might be lower than result of
    // `getNumThreads` for small inputs)
    size_t numThreadsUsed_;
    // The rows whose geometry is from the persisted store, see
    // `WKTParser::takeStoredRows`.
    std::vector<ad_utility::detail::parallel_wkt_parser::StoredGeometryRow>
        storedRows_;
  };
  struct ParseTarget {
    // The temporary store for the parsed geometries and its ID in the
    // `sj::GeometryCache`.
    sj::GeometryStore& store_;
    uint8_t storeId_;
    // The ID of the persisted store of the index in the `sj::GeometryCache`,
    // or `std::nullopt` if it is not used.
    std::optional<uint8_t> persistedStoreId_;
    sj::SweepEventList& events_;
  };
  ParseMetadata parse(ParseInput input, ParseTarget target, size_t numThreads,
                      std::optional<util::geo::I32Box> prefilterBox) const;

  // Prepare a libspatialjoin `SpatialJoinCfg`. The result doesn't have any of
  // its callbacks set yet. Before constructing the `sj::GeometryStore`, the
  // `sj::SweepEventList`s, the `sj::GeometryCache` and the `sj::Sweeper` (each
  // of them keeps a copy of the configuration), you usually want to set
  // `writeRelCb` and `sweepCancellationCb`, and `withinDist` for a proximity
  // search. This function is only `public` for testing purposes and should
  // otherwise not be used outside of this class.
  static sj::SpatialJoinCfg sweeperConfig(
      size_t threads, ad_utility::MemorySize totalAllowedMemory = 8_GB);

  // Check the bounding box (only if available from a `GeoVocabulary`) of a
  // given vocabulary entry against `prefilterLatLngBox`. Returns `true` if
  // the geometry can be discarded just by the bounding box. If a
  // `precomputedBoundingBox` is already available (for example from an
  // `IdTable` with dedicated bounding-box columns), it is used instead of
  // looking up the `GeoVocabulary`. Otherwise this should only be called if
  // the index is known to be built on a `GeoVocabulary`. Used by
  // `ad_utility::detail::parallel_wkt_parser::WKTParser` (see
  // `SpatialJoinParser.cpp`), which only ever parses WKT for this algorithm.
  static bool prefilterGeoByBoundingBox(
      const std::optional<::util::geo::DBox>& prefilterLatLngBox,
      const Index& index, VocabIndex vocabIndex,
      const std::optional<ad_utility::BoundingBox>& precomputedBoundingBox);

 private:
  // Directory and filename prefix for the temporary files written by
  // `libspatialjoin` during `run()`.
  struct SweeperTempPath {
    std::string dir_;
    std::string prefix_;
  };

  // Determine `SweeperTempPath` for the index of `qec_`. The directory
  // defaults to the parent directory of the index's on-disk base path, but
  // can be overridden via the `spatial-join-tmp-dir` runtime parameter.
  SweeperTempPath getSweeperTempPath() const;

  // Maximum area of bounding box in square coordinates for prefiltering
  // libspatialjoin input by bounding box. If exceeded, prefiltering is
  // disabled. See `parse`.
  static double maxAreaPrefilterBox();

  // Helper for `parse` to get the bounding box from an `IdTable` if
  // available.
  static std::optional<ad_utility::BoundingBox> getBoundingBoxFromIdTable(
      const IdTableView<0>* idTable,
      const SpatialJoinBoundingBoxColumns& boundingBoxes, size_t row);

  // Retrieve the number of threads to be used for `parse` and `run`.
  static size_t getNumThreads();

  // After adding the given amount of rows to the WKT parser, it will be checked
  // if the user has cancelled their query.
  static constexpr size_t wktParserChunkSizeForCancellationCheck = 10'000;

  SpatialJoinBoundingBoxColumns boundingBoxColsLeft_;
  SpatialJoinBoundingBoxColumns boundingBoxColsRight_;
};

#endif  // QLEVER_SRC_ENGINE_LIBSPATIALJOINALGORITHM_H
