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

#include <spatialjoin/Sweeper.h>
#include <util/geo/Geo.h>

#include <string>

#include "engine/spatialJoinAlgorithms/SpatialJoinAlgorithmBase.h"
#include "index/Index.h"
#include "util/MemorySize/MemorySize.h"

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

  // This helper functions parses WKT geometries from the given `column` in
  // `idTable` and adds them to `sweeper` (which will be used to perform the
  // spatial join). The Boolean `leftOrRightSide` specifies whether these
  // geometries are from the left or right side of the spatial join. The parsing
  // is multithreaded, using up to `numThreads` threads. If a `prefilterBox` is
  // given, geometries not intersecting this box will neither be parsed nor
  // added to `sweeper`. The function returns the aggregated bounding box of all
  // added geometries, which may be used as a prefilter at next call and the
  // number of geometries added. This function is only `public` for testing
  // purposes and should otherwise not be used outside of this class.
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
    // Actual number of threads used (might be lower than result of
    // `getNumThreads` for small inputs)
    size_t numThreadsUsed_;
  };
  ParseMetadata parse(bool leftOrRightSide, ParseInput input,
                      sj::Sweeper& sweeper, size_t numThreads,
                      std::optional<util::geo::I32Box> prefilterBox) const;

  // Prepare a libspatialjoin `SweeperCfg`. The result doesn't have any of its
  // callbacks set yet. Before feeding the configuration to a `Sweeper` you
  // usually want to set `writeRelCb` and `sweepCancellationCb`. Also
  // `withinDist` should be set if a proximity search is intended. This
  // function is only `public` for testing purposes and should otherwise not
  // be used outside of this class.
  static sj::SweeperCfg sweeperConfig(
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
  // Directory and filename prefix for the temporary files written by the
  // `libspatialjoin` sweeper during `run()`.
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
