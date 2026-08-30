// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2025        Patrick Brosi <brosi@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/spatialJoinAlgorithms/LibspatialjoinAlgorithm.h"

#include <spatialjoin/BoxIds.h>

#include <thread>

#include "backports/filesystem.h"
#include "engine/IndexScan.h"
#include "engine/SpatialJoinParser.h"
#include "global/RuntimeParameters.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/ChunkedForLoop.h"
#include "util/Exception.h"
#include "util/Timer.h"

// ____________________________________________________________________________
double LibspatialjoinAlgorithm::maxAreaPrefilterBox() {
  return static_cast<double>(
      getRuntimeParameter<&RuntimeParameters::spatialJoinPrefilterMaxSize_>());
}

// ____________________________________________________________________________
std::optional<ad_utility::BoundingBox>
LibspatialjoinAlgorithm::getBoundingBoxFromIdTable(
    const IdTableView<0>* idTable,
    const SpatialJoinBoundingBoxColumns& boundingBoxes, size_t row) {
  if (!boundingBoxes.has_value()) {
    return std::nullopt;
  }
  auto idLowerLeft = idTable->at(row, boundingBoxes.value().first);
  auto idUpperRight = idTable->at(row, boundingBoxes.value().second);
  if (idLowerLeft.getDatatype() != Datatype::GeoPoint ||
      idUpperRight.getDatatype() != Datatype::GeoPoint) {
    return std::nullopt;
  }
  return ad_utility::BoundingBox{idLowerLeft.getGeoPoint(),
                                 idUpperRight.getGeoPoint()};
}

// ____________________________________________________________________________
bool LibspatialjoinAlgorithm::prefilterGeoByBoundingBox(
    const std::optional<::util::geo::DBox>& prefilterLatLngBox,
    const Index& index, VocabIndex vocabIndex,
    const std::optional<ad_utility::BoundingBox>& precomputedBoundingBox) {
  if (prefilterLatLngBox.has_value()) {
    auto hasNoIntersection =
        [&prefilterLatLngBox](const ad_utility::BoundingBox& geomBoundingBox) {
          return !::util::geo::intersects(
              prefilterLatLngBox.value(),
              ad_utility::detail::boundingBoxToUtilBox(geomBoundingBox));
        };

    // Use the `precomputedBoundingBox` for filtering if available.
    if (precomputedBoundingBox.has_value()) {
      return hasNoIntersection(precomputedBoundingBox.value());
    }

    // Otherwise, use the `GeoVocabulary` for filtering.
    auto geoInfo = index.getVocab().getGeoInfo(vocabIndex);
    if (geoInfo.has_value()) {
      // We have a bounding box: Check intersection with prefilter box.
      return hasNoIntersection(geoInfo.value().getBoundingBox());
    } else {
      // Since we know that this function is only called if we have a
      // `GeoVocabulary`, we know that a geometry without precomputed bounding
      // box must be invalid and can thus be skipped.
      return true;
    }
  }
  // If we don't have the required information, we cannot discard the geometry.
  return false;
}

// ____________________________________________________________________________
size_t LibspatialjoinAlgorithm::getNumThreads() {
  size_t maxHwConcurrency = std::thread::hardware_concurrency();
  size_t userPreference =
      getRuntimeParameter<&RuntimeParameters::spatialJoinMaxNumThreads_>();
  if (userPreference == 0 || maxHwConcurrency < userPreference) {
    return maxHwConcurrency;
  }
  return userPreference;
}

// ____________________________________________________________________________
LibspatialjoinAlgorithm::SweeperTempPath
LibspatialjoinAlgorithm::getSweeperTempPath() const {
  auto basePath = ql::filesystem::path(qec_->getIndex().getOnDiskBase());

  std::string dir =
      getRuntimeParameter<&RuntimeParameters::spatialJoinTmpDir_>();
  if (dir.empty()) {
    dir = basePath.parent_path().string();
    // `parent_path()` returns `""` if the parent path is empty, not `"."`.
    if (dir.empty()) {
      dir = ".";
    }
  }

  std::string baseName = ql::pathFilename(basePath).string();

  // The prefix added before each spatialjoin file.
  //
  // NOTE: If `getOnDiskBase()` ends with `/` or is empty, `baseName` is empty
  // and the spatialjoin files end up named `.spatialjoin`. We should consider
  // disallowing empty index base names at the engine boundary.
  std::string prefix = baseName + ".spatialjoin";

  return {std::move(dir), std::move(prefix)};
}

// ____________________________________________________________________________
sj::SweeperCfg LibspatialjoinAlgorithm::sweeperConfig(
    size_t threads, ad_utility::MemorySize totalAllowedMemory) {
  using enum SpatialJoinType::Enum;
  // `libspatialjoin` reports a match for one of these relations by invoking
  // `writeRelCb` (see below) with a `pred` argument equal to the
  // corresponding `sep...` string set below. These strings are otherwise
  // opaque to `libspatialjoin`, so any distinct single byte per relation
  // works; we simply (ab)use the (small) numeric value of the enum, which is
  // not meant to be human-readable.
  auto sep = [](SpatialJoinType type) {
    return std::string{static_cast<char>(type.value())};
  };
  AD_CORRECTNESS_CHECK(threads > 0);

  sj::SweeperCfg cfg;
  cfg.numThreads = threads;
  cfg.numCacheThreads = threads;
  // Cache memory per thread, in bytes
  cfg.geomCacheMaxSize = totalAllowedMemory.getBytes() / threads;
  cfg.geomCacheMaxNumElements = 10'000;
  cfg.sepIsect = sep(INTERSECTS);
  cfg.sepContains = sep(CONTAINS);
  cfg.sepCovers = sep(COVERS);
  cfg.sepTouches = sep(TOUCHES);
  cfg.sepEquals = sep(EQUALS);
  cfg.sepOverlaps = sep(OVERLAPS);
  cfg.sepCrosses = sep(CROSSES);
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
LibspatialjoinAlgorithm::ParseMetadata LibspatialjoinAlgorithm::parse(
    bool leftOrRightSide, ParseInput input, sj::Sweeper& sweeper,
    size_t numThreads, std::optional<util::geo::I32Box> prefilterBox) const {
  const auto [idTable, column, boundingBoxes] = input;

  // Convert prefilter box to lat lng coordinates for comparing against geometry
  // info from vocabulary.
  std::optional<util::geo::DBox> prefilterLatLngBox = std::nullopt;
  if (prefilterBox.has_value()) {
    prefilterLatLngBox = ad_utility::detail::projectInt32WebMercToDoubleLatLng(
        prefilterBox.value());
  }
  bool usePrefiltering = prefilterLatLngBox.has_value() &&
                         (boundingBoxes.has_value() ||
                          qec_->getIndex().getVocab().isGeoInfoAvailable());

  // If the prefilter box is too large, the prefiltering overhead (cost of
  // retrieving bounding boxes from disk) is likely larger than its performance
  // gain. Therefore prefiltering is disabled in this case.
  if (usePrefiltering &&
      util::geo::area(prefilterLatLngBox.value()) > maxAreaPrefilterBox()) {
    usePrefiltering = false;
    spatialJoin_.value()->runtimeInfo().addDetail(
        "prefilter-disabled-by-bounding-box-area", true);
  }

  // If the input is smaller than one batch for every thread, reduce the number
  // of threads accordingly to avoid spawning threads that will never be used.
  static constexpr auto batchSize =
      ad_utility::detail::parallel_wkt_parser::WKT_PARSER_BATCH_SIZE;
  static_assert(batchSize > 0);
  size_t requiredBatches = (idTable->size() + batchSize - 1ULL) / batchSize;
  numThreads = std::min(numThreads, requiredBatches);

  // Initialize the parser.
  ad_utility::detail::parallel_wkt_parser::WKTParser parser(
      &sweeper, numThreads, usePrefiltering, prefilterLatLngBox,
      qec_->getIndex());

  // Iterate over all rows in `idTable` and add the geometries from `column`
  // to the parallel WKT parser.
  const auto& geoms = idTable->getColumn(column);
  ad_utility::chunkedForLoop<wktParserChunkSizeForCancellationCheck>(
      0, idTable->size(),
      [&parser, &geoms, &leftOrRightSide, &idTable,
       &boundingBoxes](size_t row) {
        parser.addValueIdToQueue(
            geoms[row], row, leftOrRightSide,
            getBoundingBoxFromIdTable(idTable, boundingBoxes, row));
      },
      [this]() { throwIfCancelled(); });

  // Wait for all parser threads to finish, then return the bounding box of all
  // the geometries parsed so far.
  parser.done();

  auto numGeomsDropped = parser.getPrefilterCounter();
  auto numGeomsParsed = idTable->size() - numGeomsDropped;
  return {parser.getBoundingBox(), numGeomsParsed, numGeomsDropped,
          parser.getCellPrefilterCounter(), numThreads};
}

// ____________________________________________________________________________
Result LibspatialjoinAlgorithm::run() {
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, leftSelectedCols, rightSelectedCols, numColumns,
              numRowsBeforeBlockPrefilterLeft, numRowsBeforeBlockPrefilterRight,
              timeBlockPrefilter] = params_;
  // Setup.
  IdTable result{numColumns, qec_->getAllocator()};
  size_t NUM_THREADS = getNumThreads();
  std::vector<std::vector<std::pair<size_t, size_t>>> results(NUM_THREADS);
  std::vector<std::vector<double>> resultDists(NUM_THREADS);
  AD_CORRECTNESS_CHECK(config_.getJoinType().has_value());
  auto joinTypeVal = config_.getJoinType().value();
  // Within should be replaced by contains on swapped tables.
  auto swapBack = joinTypeVal == SpatialJoinType::WITHIN;
  if (swapBack) {
    joinTypeVal = SpatialJoinType::CONTAINS;
  }

  // Configure the sweeper.
  sj::SweeperCfg sweeperCfg =
      sweeperConfig(NUM_THREADS, qec_->getAllocator().amountMemoryLeft());
  if (joinTypeVal == SpatialJoinType::WITHIN_DIST) {
    AD_CORRECTNESS_CHECK(maxDist_.has_value());
    sweeperCfg.withinDist = maxDist_.value();
  }
  // For the `DE9IM` join type, let `libspatialjoin` compute the full DE-9IM
  // matrix for every candidate pair and only report those matching the
  // user-provided filter pattern.
  if (joinTypeVal == SpatialJoinType::DE9IM) {
    auto de9imFilter = spatialJoin_.value()->getDe9imFilter();
    AD_CORRECTNESS_CHECK(de9imFilter.has_value());
    sweeperCfg.computeDE9IM = true;
    sweeperCfg.de9imFilter = ::util::geo::DE9IMFilter(de9imFilter->data());
    spatialJoin_.value()->runtimeInfo().addDetail(
        "de9im-filter", sweeperCfg.de9imFilter.toString());
  }
  if (joinTypeVal == SpatialJoinType::WITHIN_DIST) {
    sweeperCfg.writeRelCb = [&results, &resultDists](
                                size_t t, const char* a, size_t, const char* b,
                                size_t, const char* pred, size_t) {
      results[t].push_back({std::atoi(a), std::atoi(b)});
      resultDists[t].push_back(atof(pred));
    };
  } else if (joinTypeVal == SpatialJoinType::DE9IM) {
    // `libspatialjoin` only invokes this callback for pairs that already
    // matched `sweeperCfg.de9imFilter`.
    sweeperCfg.writeRelCb = [&results](size_t t, const char* a, size_t,
                                       const char* b, size_t, const char*,
                                       size_t) {
      results[t].push_back({std::atoi(a), std::atoi(b)});
    };
  } else {
    sweeperCfg.writeRelCb = [&results, joinTypeVal](
                                size_t t, const char* a, size_t, const char* b,
                                size_t, const char* pred, size_t) {
      if (pred[0] == static_cast<char>(joinTypeVal.value())) {
        results[t].push_back({std::atoi(a), std::atoi(b)});
      }
    };
  }
  sweeperCfg.sweepCancellationCb = [this]() { throwIfCancelled(); };

  auto [sweeperTmpPath, sweeperPrefix] = getSweeperTempPath();

  sj::Sweeper sweeper(sweeperCfg, sweeperTmpPath, sweeperPrefix);
  ad_utility::Timer tParse{ad_utility::Timer::Started};

  // Parse the geometries from the left and right input table, starting with the
  // smaller one. Compute the bounding box of the smaller table (appropriately
  // inflated for `WITHIN_DIST` joins) and only add those geometries from the
  // larger table that intersect this bounding box.
  size_t numParserThreadsSmaller = 0;
  size_t numParserThreadsLarger = 0;
  auto runParser = [&](ParseInput smaller, ParseInput larger,
                       bool smallerIsRight) {
    // Report the geometry funnel of the larger side in the chronological
    // order of the pruning stages: how many geometries survive each stage.
    // The "before block prefilter" value is only exactly known when the
    // larger side is a (possibly view-backed) index scan whose blocks were
    // pruned; otherwise it equals the delivered row count.
    uint64_t numGeomsAfterBlockPrefilter = larger.idTable_->size();
    // The larger side is the left one iff the smaller side is the right one.
    uint64_t numGeomsBeforeBlockPrefilter =
        (smallerIsRight ? numRowsBeforeBlockPrefilterLeft
                        : numRowsBeforeBlockPrefilterRight)
            .value_or(numGeomsAfterBlockPrefilter);
    spatialJoin_.value()->runtimeInfo().addDetail(
        "num-geoms-before-block-prefilter", numGeomsBeforeBlockPrefilter);
    spatialJoin_.value()->runtimeInfo().addDetail(
        "num-geoms-after-block-prefilter", numGeomsAfterBlockPrefilter);

    // Parse and add all geometries of the smaller side
    auto [boxSmall, countSmall, droppedSmall, droppedSmallByCell,
          threadsSmall] =
        parse(smallerIsRight, smaller, sweeper, NUM_THREADS, std::nullopt);
    AD_CORRECTNESS_CHECK(droppedSmall == 0);
    auto numValidGeomsSmall = sweeper.numElements();

    // Filtering by bounding box *after* parsing is only necessary if
    // precomputed bounding boxes for filtering *before* parsing are not
    // available.
    if (!qec_->getIndex().getVocab().isGeoInfoAvailable()) {
      sweeper.setFilterBox(boxSmall);
    }

    // Parse and add the relevant (intersection with the bounding box)
    // geometries from the larger side
    auto [boxLarge, countLarge, droppedLarge, droppedLargeByCell,
          threadsLarge] = parse(!smallerIsRight, larger, sweeper, NUM_THREADS,
                                sweeper.getPaddedBoundingBox(boxSmall));
    auto numValidGeomsTotal = sweeper.numElements();
    AD_CORRECTNESS_CHECK(numValidGeomsTotal >= numValidGeomsSmall);
    auto numValidGeomsLarge = numValidGeomsTotal - numValidGeomsSmall;

    spatialJoin_.value()->runtimeInfo().addDetail(
        "num-geoms-after-cell-prefilter",
        numGeomsAfterBlockPrefilter - droppedLargeByCell);
    spatialJoin_.value()->runtimeInfo().addDetail(
        "num-geoms-after-bbox-prefilter",
        numGeomsAfterBlockPrefilter - droppedLarge);
    spatialJoin_.value()->runtimeInfo().addDetail("num-valid-geoms-parsed",
                                                  numValidGeomsTotal);
    numParserThreadsSmaller = threadsSmall;
    numParserThreadsLarger = threadsLarge;

    // If we have filtered out all geometries or one side is otherwise empty,
    // bail out early.
    return numValidGeomsSmall > 0 && numValidGeomsLarge > 0;
  };

  ParseInput leftTableAndCol{idTableLeft, leftJoinCol, boundingBoxColsLeft_};
  ParseInput rightTableAndCol{idTableRight, rightJoinCol,
                              boundingBoxColsRight_};
  bool nonEmptyChildren =
      idTableLeft->size() < idTableRight->size()
          ? runParser(leftTableAndCol, rightTableAndCol, false)
          : runParser(rightTableAndCol, leftTableAndCol, true);

  // The four time details come after the geometry funnel, in the
  // chronological order of the steps they time.
  spatialJoin_.value()->runtimeInfo().addDetail("time-for-prefiltering-blocks",
                                                timeBlockPrefilter.count());

  // Flush the geometry caches and the sweepline event list cache to disk and
  // add the time for parsing and processing the geometries to the runtime
  // information.
  sweeper.flush();
  spatialJoin_.value()->runtimeInfo().addDetail("time-for-reading-geometries",
                                                tParse.msecs().count());

  // Now do the sweep, which performs the actual spatial join.
  ad_utility::Timer tSweep{ad_utility::Timer::Started};
  // The check for empty children is required to mitigate a libspatialjoin bug,
  // but also for performance reasons.
  if (nonEmptyChildren) {
    sweeper.sweep();
  }
  spatialJoin_.value()->runtimeInfo().addDetail("time-for-spatialjoin-sweep",
                                                tSweep.msecs().count());
  ad_utility::Timer tCollect{ad_utility::Timer::Started};

  // Collect the results and add them to the result table. For `WITHIN_DIST`,
  // also add the distance for each pair of objects in the result.
  for (size_t t = 0; t < NUM_THREADS; t++) {
    for (size_t i = 0; i < results[t].size(); i++) {
      throwIfCancelled();

      const auto& res = results[t][i];
      double dist = 0;
      if (joinTypeVal == SpatialJoinType::WITHIN_DIST) {
        dist = resultDists[t][i];
      }
      addResultTableEntry(&result, idTableLeft, idTableRight, res.first,
                          res.second, Id::makeFromDouble(dist), swapBack);
    }
  }
  spatialJoin_.value()->runtimeInfo().addDetail(
      "time-for-collecting-results-from-threads", tCollect.msecs().count());

  // The configuration details (thread counts, distance) come last.
  spatialJoin_.value()->runtimeInfo().addDetail("num-sweeper-threads",
                                                NUM_THREADS);
  if (joinTypeVal == SpatialJoinType::WITHIN_DIST) {
    spatialJoin_.value()->runtimeInfo().addDetail("within-dist",
                                                  sweeperCfg.withinDist);
  }
  spatialJoin_.value()->runtimeInfo().addDetail(
      "num-parser-threads-smaller-side", numParserThreadsSmaller);
  spatialJoin_.value()->runtimeInfo().addDetail(
      "num-parser-threads-larger-side", numParserThreadsLarger);

  // Return the result.
  return Result(std::move(result), std::vector<ColumnIndex>{},
                Result::getMergedLocalVocab(*resultLeft, *resultRight));
}
