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

#include "backports/filesystem.h"
#include "engine/SpatialJoinParser.h"
#include "engine/spatialJoinAlgorithms/SpatialJoinGeoUtils.h"
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
LibspatialjoinAlgorithm::LibSpatialJoinParseMetadata
LibspatialjoinAlgorithm::libspatialjoinParse(
    bool leftOrRightSide, LibSpatialJoinParseInput input, sj::Sweeper& sweeper,
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
            ad_utility::detail::spatialjoin::getBoundingBoxFromIdTable(
                idTable, boundingBoxes, row));
      },
      [this]() { throwIfCancelled(); });

  // Wait for all parser threads to finish, then return the bounding box of all
  // the geometries parsed so far.
  parser.done();

  auto numGeomsDropped = parser.getPrefilterCounter();
  auto numGeomsParsed = idTable->size() - numGeomsDropped;
  return {parser.getBoundingBox(), numGeomsParsed, numGeomsDropped, numThreads};
}

// ____________________________________________________________________________
Result LibspatialjoinAlgorithm::run() {
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, leftSelectedCols, rightSelectedCols, numColumns,
              maxDist, maxResults, joinType, de9imFilter, rightCacheName,
              bbLeft, bbRight] = params_;
  // Setup.
  IdTable result{numColumns, qec_->getAllocator()};
  size_t NUM_THREADS = ad_utility::detail::spatialjoin::getNumThreads();
  std::vector<std::vector<std::pair<size_t, size_t>>> results(NUM_THREADS);
  std::vector<std::vector<double>> resultDists(NUM_THREADS);
  auto joinTypeVal = joinType.value_or(SpatialJoinType::INTERSECTS);
  // Within should be replaced by contains on swapped tables.
  auto swapBack = joinTypeVal == SpatialJoinType::WITHIN;
  if (swapBack) {
    joinTypeVal = SpatialJoinType::CONTAINS;
  }

  // Add number of threads to runtime information.
  spatialJoin_.value()->runtimeInfo().addDetail("num-sweeper-threads",
                                                NUM_THREADS);

  // Set the distance for the `WITHIN_DIST` join type. This has to be set to
  // a value < 0 to disable the `WITHIN_DIST` calculation in `libspatialjoin`.
  double withinDist = -1;
  if (joinTypeVal == SpatialJoinType::WITHIN_DIST) {
    withinDist = maxDist.value_or(0);
    spatialJoin_.value()->runtimeInfo().addDetail("within-dist", withinDist);
  }

  // Configure the sweeper.
  sj::SweeperCfg sweeperCfg =
      ad_utility::detail::spatialjoin::libspatialjoinSweeperConfig(
          NUM_THREADS, qec_->getAllocator().amountMemoryLeft());
  sweeperCfg.withinDist = withinDist;
  // For the `DE9IM` join type, let `libspatialjoin` compute the full DE-9IM
  // matrix for every candidate pair and only report those matching the
  // user-provided filter pattern.
  if (joinTypeVal == SpatialJoinType::DE9IM) {
    AD_CORRECTNESS_CHECK(de9imFilter.has_value());
    sweeperCfg.computeDE9IM = true;
    sweeperCfg.de9imFilter = ::util::geo::DE9IMFilter(de9imFilter->data());
  }
  sweeperCfg.writeRelCb = [&results, &resultDists, joinTypeVal](
                              size_t t, const char* a, size_t, const char* b,
                              size_t, const char* pred, size_t) {
    if (joinTypeVal == SpatialJoinType::WITHIN_DIST) {
      results[t].push_back({std::atoi(a), std::atoi(b)});
      resultDists[t].push_back(atof(pred));
    } else if (joinTypeVal == SpatialJoinType::DE9IM) {
      // `libspatialjoin` only invokes this callback for pairs that already
      // matched `sweeperCfg.de9imFilter`.
      results[t].push_back({std::atoi(a), std::atoi(b)});
    } else if (pred[0] == static_cast<char>(joinTypeVal.value())) {
      results[t].push_back({std::atoi(a), std::atoi(b)});
    }
  };
  sweeperCfg.sweepCancellationCb = [this]() { throwIfCancelled(); };

  auto basePath = ql::filesystem::path(qec_->getIndex().getOnDiskBase());

  std::string sweeperTmpPath = basePath.parent_path().string();

  // `parent_path()` returns `""` if the parent path is empty, not `"."`.
  if (sweeperTmpPath.empty()) {
    sweeperTmpPath = ".";
  }

  std::string baseName = ql::pathFilename(basePath).string();

  // The prefix added before each spatialjoin file.
  //
  // NOTE: If `getOnDiskBase()` ends with `/` or is empty, `baseName` is empty
  // and the spatialjoin files end up named `.spatialjoin`. We should consider
  // disallowing empty index base names at the engine boundary.
  std::string sweeperPrefix = baseName + ".spatialjoin";

  sj::Sweeper sweeper(sweeperCfg, sweeperTmpPath, sweeperPrefix);
  ad_utility::Timer tParse{ad_utility::Timer::Started};

  // Parse the geometries from the left and right input table, starting with the
  // smaller one. Compute the bounding box of the smaller table (appropriately
  // inflated for `WITHIN_DIST` joins) and only add those geometries from the
  // larger table that intersect this bounding box.
  auto runParser = [&](LibSpatialJoinParseInput smaller,
                       LibSpatialJoinParseInput larger, bool smallerIsRight) {
    // Parse and add all geometries of the smaller side
    auto [boxSmall, countSmall, droppedSmall, threadsSmall] =
        libspatialjoinParse(smallerIsRight, smaller, sweeper, NUM_THREADS,
                            std::nullopt);
    AD_CORRECTNESS_CHECK(droppedSmall == 0);
    spatialJoin_.value()->runtimeInfo().addDetail(
        "num-parser-threads-smaller-side", threadsSmall);
    auto numValidGeomsSmall = sweeper.numElements();

    // Filtering by bounding box *after* parsing is only necessary if
    // precomputed bounding boxes for filtering *before* parsing are not
    // available.
    if (!qec_->getIndex().getVocab().isGeoInfoAvailable()) {
      sweeper.setFilterBox(boxSmall);
    }

    // Parse and add the relevant (intersection with the bounding box)
    // geometries from the larger side
    auto [boxLarge, countLarge, droppedLarge, threadsLarge] =
        libspatialjoinParse(!smallerIsRight, larger, sweeper, NUM_THREADS,
                            sweeper.getPaddedBoundingBox(boxSmall));
    auto numValidGeomsTotal = sweeper.numElements();
    AD_CORRECTNESS_CHECK(numValidGeomsTotal >= numValidGeomsSmall);
    auto numValidGeomsLarge = numValidGeomsTotal - numValidGeomsSmall;

    spatialJoin_.value()->runtimeInfo().addDetail(
        "num-parser-threads-larger-side", threadsLarge);
    spatialJoin_.value()->runtimeInfo().addDetail("num-geoms-parsed",
                                                  countSmall + countLarge);
    spatialJoin_.value()->runtimeInfo().addDetail("num-valid-geoms-parsed",
                                                  numValidGeomsTotal);
    spatialJoin_.value()->runtimeInfo().addDetail(
        "num-geoms-dropped-by-prefilter", droppedLarge);

    // If we have filtered out all geometries or one side is otherwise empty,
    // bail out early.
    return numValidGeomsSmall > 0 && numValidGeomsLarge > 0;
  };

  LibSpatialJoinParseInput leftTableAndCol{idTableLeft, leftJoinCol, bbLeft};
  LibSpatialJoinParseInput rightTableAndCol{idTableRight, rightJoinCol,
                                            bbRight};
  bool nonEmptyChildren =
      idTableLeft->size() < idTableRight->size()
          ? runParser(leftTableAndCol, rightTableAndCol, false)
          : runParser(rightTableAndCol, leftTableAndCol, true);

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

  // Return the result.
  return Result(std::move(result), std::vector<ColumnIndex>{},
                Result::getMergedLocalVocab(*resultLeft, *resultRight));
}
