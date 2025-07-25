// Copyright 2024 - 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINPARSER_H_
#define QLEVER_SRC_ENGINE_SPATIALJOINPARSER_H_

#include <spatialjoin/Sweeper.h>
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

class WKTParser {
 public:
  WKTParser(sj::Sweeper* sweeper, size_t numThreads, bool usePrefiltering,
            const std::optional<util::geo::DBox>& prefilterLatLngBox,
            const Index& index);
  ~WKTParser();

  void addValueIdToQueue(ValueId valueId, size_t id, bool side);

  util::geo::I32Box getBoundingBox() const { return _bbox; }

  void done();

  static util::geo::I32Point projFunc(const util::geo::DPoint& p) {
    auto projPoint = latLngToWebMerc(p);
    return {static_cast<int>(projPoint.getX() * PREC),
            static_cast<int>(projPoint.getY() * PREC)};
  }

  size_t getPrefilterCounter();

 private:
  void parseLine(char* c, size_t len, size_t gid, size_t t,
                 sj::WriteBatch& batch, bool side);
  void processQueue(size_t t);

  size_t _gid = 1;
  std::string _dangling;

  sj::Sweeper* _sweeper;

  ParseBatch _curBatch;

  util::geo::I32Box _bbox;

  util::JobQueue<ParseBatch> _jobs;
  std::vector<std::thread> _thrds;

  std::vector<util::geo::I32Box> _bboxes;

  std::atomic<bool> _cancelled;

  std::vector<size_t> _numSkipped;

  bool _usePrefiltering;
  const std::optional<util::geo::DBox>& _prefilterLatLngBox;
  const Index& _index;
};

}  // namespace sjTemp

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINPARSER_H_
