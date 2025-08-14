// Copyright 2024 - 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINPARSER_H_
#define QLEVER_SRC_ENGINE_SPATIALJOINPARSER_H_

#include <spatialjoin/Sweeper.h>
#include <spatialjoin/WKTParse.h>
#include <util/geo/Geo.h>
#include <util/log/Log.h>

#include <atomic>

#include "global/ValueId.h"
#include "index/Index.h"

namespace ad_utility::detail::parallel_wkt_parser {

// Special parse job using `ValueId` instead of string.
struct SpatialJoinParseJob {
  ValueId valueId;
  size_t line;
  bool side;
  std::string wkt;
};

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

  void addValueIdToQueue(ValueId valueId, size_t id, bool side);

  size_t getPrefilterCounter();
  size_t getParseCounter();

 protected:
  void processQueue(size_t t) override;

 private:
  std::vector<size_t> _numSkipped;
  std::vector<size_t> _numParsed;

  bool _usePrefiltering;
  std::optional<util::geo::DBox> _prefilterLatLngBox;
  const Index& _index;
};

}  // namespace ad_utility::detail::parallel_wkt_parser

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINPARSER_H_
