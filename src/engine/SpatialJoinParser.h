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

namespace sjTemp {

struct ParseJob {
  ValueId valueId;
  size_t line;
  bool side;
  std::string wkt;
};

inline bool operator==(const ParseJob& a, const ParseJob& b) {
  return a.line == b.line && a.valueId == b.valueId && a.side == b.side;
}

typedef std::vector<ParseJob> ParseBatch;

class WKTParser : public sj::WKTParserBase<ParseJob> {
 public:
  WKTParser(sj::Sweeper* sweeper, size_t numThreads, bool usePrefiltering,
            const std::optional<util::geo::DBox>& prefilterLatLngBox,
            const Index& index);

  void addValueIdToQueue(ValueId valueId, size_t id, bool side);

  size_t getPrefilterCounter();

 protected:
  void processQueue(size_t t) override;

 private:
  std::vector<size_t> _numSkipped;

  bool _usePrefiltering;
  const std::optional<util::geo::DBox>& _prefilterLatLngBox;
  const Index& _index;
};

}  // namespace sjTemp

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINPARSER_H_
