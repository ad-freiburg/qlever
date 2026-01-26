// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINPARSER_H_
#define QLEVER_SRC_ENGINE_SPATIALJOINPARSER_H_

#include <spatialjoin/Sweeper.h>
#include <spatialjoin/WKTParse.h>
#include <util/geo/Geo.h>
#include <util/log/Log.h>

#include "global/ValueId.h"
#include "index/Index.h"

namespace ad_utility::detail::parallel_wkt_parser {

// The number of geometries per batch passed to a thread.
constexpr inline size_t WKT_PARSER_BATCH_SIZE = 10'000;

// Special parse job using `ValueId` instead of string.
struct SpatialJoinParseJob {
  ValueId valueId;
  size_t line;
  bool side;
  std::string wkt;
};

// Compare two `SpatialJoinParseJob` objects. The member attribute `wkt` is used
// only as an internal buffer during processing and is otherwise empty,
// therefore it is not compared here.
inline bool operator==(const SpatialJoinParseJob& a,
                       const SpatialJoinParseJob& b) {
  return a.line == b.line && a.valueId == b.valueId && a.side == b.side;
}

// Custom parallel WKT parser, which receives only `ValueId`s instead of
// literals and fetches the corresponding bounding boxes and literals from the
// vocabulary on the fly (and in parallel).
class WKTParser : public sj::WKTParserBase<SpatialJoinParseJob> {
 public:
  WKTParser(sj::Sweeper* sweeper, size_t numThreads, bool usePrefiltering,
            const std::optional<util::geo::DBox>& prefilterLatLngBox,
            const Index& index);

  // Enqueue a new row from the input table (given the `ValueId` of the
  // geometry: `GeoPoint` or `VocabIndex` or `LocalVocabIndex`, the `rowIndex`
  // in the input table `id` and whether the geometry should be assigned to the
  // left or right `side` of the spatial join)
  void addValueIdToQueue(ValueId valueId, size_t rowIndex, bool side);

  // Accumulate the counters across all threads. They count the number of
  // geometries skipped by bounding box prefilter and the number of parsed (that
  // is, not skipped) geometries respectively.
  size_t getPrefilterCounter();
  size_t getParseCounter();

 protected:
  void processQueue(size_t t) override;

 private:
  // Members are named `_member`, not `member_` for consistency with the base
  // class from `libspatialjoin`.

  // The vectors `_numSkipped` and `_numParsed` hold the number of geometries
  // that were skipped by prefilter or actually parsed for each of the threads.
  std::vector<size_t> _numSkipped;
  std::vector<size_t> _numParsed;

  // Configure prefiltering geometries by bounding box.
  bool _usePrefiltering;
  std::optional<util::geo::DBox> _prefilterLatLngBox;

  // A reference to QLever's index is needed to access precomputed geometry
  // bounding boxes and to resolve `ValueId`s into WKT literals.
  const Index& _index;
};

}  // namespace ad_utility::detail::parallel_wkt_parser

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINPARSER_H_
