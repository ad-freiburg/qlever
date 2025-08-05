// Copyright 2024 - 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/SpatialJoinParser.h"

#include "engine/SpatialJoinAlgorithms.h"

namespace sjTemp {

using util::geo::collectionFromWKT;
using util::geo::getWKTType;
using util::geo::lineFromWKT;
using util::geo::multiLineFromWKT;
using util::geo::multiPointFromWKT;
using util::geo::multiPolygonFromWKT;
using util::geo::pointFromWKT;
using util::geo::polygonFromWKT;

// _____________________________________________________________________________
WKTParser::WKTParser(sj::Sweeper* sweeper, size_t numThreads,
                     bool usePrefiltering,
                     const std::optional<util::geo::DBox>& prefilterLatLngBox,
                     const Index& index)
    : sj::WKTParserBase<ParseJob>(sweeper, numThreads),
      _numSkipped(numThreads),
      _usePrefiltering(usePrefiltering),
      _prefilterLatLngBox(prefilterLatLngBox),
      _index(index) {
  for (size_t i = 0; i < _thrds.size(); i++) {
    _thrds[i] = std::thread(&WKTParser::processQueue, this, i);
  }
}

// _____________________________________________________________________________
size_t WKTParser::getPrefilterCounter() {
  size_t res = 0;
  for (auto c : _numSkipped) {
    res += c;
  }
  return res;
};

// _____________________________________________________________________________
void WKTParser::processQueue(size_t t) {
  ParseBatch batch;
  size_t prefilterCounter = 0;
  while ((batch = _jobs.get()).size()) {
    sj::WriteBatch w;
    for (auto& job : batch) {
      if (_cancelled) break;

      auto dt = job.valueId.getDatatype();
      if (dt == Datatype::VocabIndex) {
        // If we have a prefilter box, check if we also have a precomputed
        // bounding box for the geometry this `VocabIndex` is referring to.
        if (_usePrefiltering &&
            SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
                _prefilterLatLngBox, _index, job.valueId.getVocabIndex())) {
          prefilterCounter++;
          continue;
        }

        // If we have not filtered out this geometry, read and parse the full
        // string.
        job.wkt = _index.indexToString(job.valueId.getVocabIndex());
        parseLine(const_cast<char*>(job.wkt.c_str()), job.wkt.size(), job.line,
                  t, w, job.side);
      } else if (dt == Datatype::GeoPoint) {
        const auto& p = job.valueId.getGeoPoint();
        const util::geo::DPoint utilPoint{p.getLng(), p.getLat()};

        // If point is not contained in the prefilter box, we can skip it
        // immediately instead of feeding it to the parser.
        if (_prefilterLatLngBox.has_value() &&
            !util::geo::intersects(_prefilterLatLngBox.value(), utilPoint)) {
          prefilterCounter++;
          continue;
        }
        // parse point directly
        auto mercPoint = latLngToWebMerc(utilPoint);

        util::geo::I32Point addPoint{static_cast<int>(mercPoint.getX() * PREC),
                                     static_cast<int>(mercPoint.getY() * PREC)};
        _bboxes[t] = util::geo::extendBox(
            _sweeper->add(addPoint, std::to_string(job.line), job.side, w),
            _bboxes[t]);
      } else if (dt == Datatype::LocalVocabIndex) {
        // `LocalVocabEntry` has to be parsed in any case: we have no
        // information except the string.
        const auto& literalOrIri = *job.valueId.getLocalVocabIndex();
        if (literalOrIri.isLiteral()) {
          job.wkt = asStringViewUnsafe(literalOrIri.getLiteral().getContent());
          parseLine(const_cast<char*>(job.wkt.c_str()), job.wkt.size(),
                    job.line, t, w, job.side);
        }
      }
    }

    _sweeper->addBatch(w);
  }

  _numSkipped[t] = prefilterCounter;
}

// _____________________________________________________________________________
void WKTParser::addValueIdToQueue(ValueId valueId, size_t id, bool side) {
  _curBatch.reserve(10000);
  _curBatch.push_back({valueId, id, side, ""});

  if (_curBatch.size() > 10000) {
    _jobs.add(std::move(_curBatch));
    _curBatch.clear();
  }
}

}  // namespace sjTemp

namespace sj {
template class WKTParserBase<sjTemp::ParseJob>;
}
