// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/SpatialJoinParser.h"

#include <range/v3/numeric/accumulate.hpp>

#include "engine/SpatialJoinAlgorithms.h"

namespace ad_utility::detail::parallel_wkt_parser {

// _____________________________________________________________________________
WKTParser::WKTParser(sj::Sweeper* sweeper, size_t numThreads,
                     bool usePrefiltering,
                     const std::optional<util::geo::DBox>& prefilterLatLngBox,
                     const Index& index)
    : sj::WKTParserBase<SpatialJoinParseJob>(sweeper, numThreads),
      _numSkipped(numThreads),
      _numParsed(numThreads),
      _usePrefiltering(usePrefiltering),
      _prefilterLatLngBox(prefilterLatLngBox),
      _index(index) {
  for (size_t i = 0; i < _thrds.size(); i++) {
    _thrds[i] = std::thread(&WKTParser::processQueue, this, i);
  }
  _curBatch.reserve(WKT_PARSER_BATCH_SIZE);
}

// _____________________________________________________________________________
size_t WKTParser::getPrefilterCounter() {
  return ::ranges::accumulate(_numSkipped, 0);
};

// _____________________________________________________________________________
size_t WKTParser::getParseCounter() {
  return ::ranges::accumulate(_numParsed, 0);
};

// _____________________________________________________________________________
void WKTParser::processQueue(size_t t) {
  std::vector<SpatialJoinParseJob> batch;
  size_t prefilterCounter = 0;
  size_t parseCounter = 0;
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
        parseLine(job.wkt.data(), job.wkt.size(), job.line, t, w, job.side,
                  false);
        parseCounter++;
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

        util::geo::I32Point addPoint{
            static_cast<int32_t>(mercPoint.getX() * PREC),
            static_cast<int32_t>(mercPoint.getY() * PREC)};
        _bboxes[t] = util::geo::extendBox(
            _sweeper->add(addPoint, std::to_string(job.line), job.side, w),
            _bboxes[t]);
        parseCounter++;
      } else if (dt == Datatype::LocalVocabIndex) {
        // `LocalVocabEntry` has to be parsed in any case: we have no
        // information except the string.
        const auto& literalOrIri = *job.valueId.getLocalVocabIndex();
        if (literalOrIri.isLiteral()) {
          job.wkt = asStringViewUnsafe(literalOrIri.getLiteral().getContent());
          parseLine(job.wkt.data(), job.wkt.size(), job.line, t, w, job.side,
                    false);
          parseCounter++;
        }
      }
    }

    _sweeper->addBatch(w);
  }

  _numSkipped[t] = prefilterCounter;
  _numParsed[t] = parseCounter;
}

// _____________________________________________________________________________
void WKTParser::addValueIdToQueue(ValueId valueId, size_t rowIndex, bool side) {
  _curBatch.push_back({valueId, rowIndex, side, ""});

  if (_curBatch.size() >= WKT_PARSER_BATCH_SIZE) {
    _jobs.add(std::move(_curBatch));
    _curBatch.clear();
    _curBatch.reserve(WKT_PARSER_BATCH_SIZE);
  }
}

}  // namespace ad_utility::detail::parallel_wkt_parser

namespace sj {
template class WKTParserBase<
    ad_utility::detail::parallel_wkt_parser::SpatialJoinParseJob>;
}
