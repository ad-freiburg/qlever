// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_ENGINE_SPATIALJOINPARSERTESTHELPERS_H
#define QLEVER_TEST_ENGINE_SPATIALJOINPARSERTESTHELPERS_H

#include <absl/strings/str_join.h>
#include <gmock/gmock.h>
#include <spatialjoin/Sweeper.h>
#include <spatialjoin/WKTParse.h>
#include <util/geo/Geo.h>

#include "./SpatialJoinPrefilterTestHelpers.h"
#include "engine/SpatialJoinParser.h"

// _____________________________________________________________________________
namespace SpatialJoinParserTestHelpers {

using namespace SpatialJoinPrefilterTestHelpers;
using namespace ad_utility::detail::parallel_wkt_parser;

// TODO: getWKTParserForTesting (build test qec/index, sweeper)

}  // namespace SpatialJoinParserTestHelpers

#endif  // QLEVER_TEST_ENGINE_SPATIALJOINPARSERTESTHELPERS_H
