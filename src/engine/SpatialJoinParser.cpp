// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/SpatialJoinParser.h"

#include <range/v3/numeric/accumulate.hpp>

#include "engine/spatialJoinAlgorithms/LibspatialjoinAlgorithm.h"
#include "util/ExceptionHandling.h"

namespace ad_utility::detail::parallel_wkt_parser {

// _____________________________________________________________________________
WKTParser::WKTParser(sj::Sweeper* sweeper, size_t numThreads,
                     bool usePrefiltering,
                     const std::optional<::util::geo::DBox>& prefilterLatLngBox,
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
WKTParser::~WKTParser() {
  ad_utility::terminateIfThrows(
      [this]() {
        _cancelled = true;
        // An empty batch is the end event of the job queue (see `done()`).
        _jobs.add({});
        for (auto& thread : _thrds) {
          if (thread.joinable()) {
            thread.join();
          }
        }
      },
      "Stopping the worker threads in the destructor of `WKTParser`");
}

// _____________________________________________________________________________
size_t WKTParser::getPrefilterCounter() {
  return ::ranges::accumulate(_numSkipped, 0);
}

// _____________________________________________________________________________
size_t WKTParser::getParseCounter() {
  return ::ranges::accumulate(_numParsed, 0);
}

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
            LibspatialjoinAlgorithm::prefilterGeoByBoundingBox(
                _prefilterLatLngBox, _index, job.valueId.getVocabIndex(),
                job.boundingBox)) {
          prefilterCounter++;
          continue;
        }

        // If we have not filtered out this geometry, read and parse the full
        // string. Invalid WKT (according to the strict parsing mode) is
        // skipped, so it never matches (see GeoSPARQL 1.1, Section 10.9.1).
        job.wkt = _index.indexToString(job.valueId.getVocabIndex());
        parseWKTGeometry(job.wkt.c_str(), std::to_string(job.line), t, w,
                         job.side, true);
        parseCounter++;
      } else if (dt == Datatype::GeoPoint) {
        const auto& p = job.valueId.getGeoPoint();
        const ::util::geo::DPoint utilPoint{p.getLng(), p.getLat()};

        // If point is not contained in the prefilter box, we can skip it
        // immediately instead of feeding it to the parser.
        if (_prefilterLatLngBox.has_value() &&
            !::util::geo::intersects(_prefilterLatLngBox.value(), utilPoint)) {
          prefilterCounter++;
          continue;
        }
        // parse point directly
        auto mercPoint = latLngToWebMerc(utilPoint);

        ::util::geo::I32Point addPoint{
            static_cast<int32_t>(mercPoint.getX() * PREC),
            static_cast<int32_t>(mercPoint.getY() * PREC)};
        _bboxes[t] = ::util::geo::extendBox(
            _sweeper->add(addPoint, std::to_string(job.line), job.side, w),
            _bboxes[t]);
        parseCounter++;
      } else if (dt == Datatype::LocalVocabIndex) {
        // `LocalVocabEntry` has to be parsed in any case: we have no
        // information except the string.
        // Invalid WKT is skipped as above.
        const auto& literalOrIri = *job.valueId.getLocalVocabIndex();
        if (literalOrIri.isLiteral()) {
          job.wkt = literalOrIri.getLiteral().getContent();
          parseWKTGeometry(job.wkt.c_str(), std::to_string(job.line), t, w,
                           job.side, true);
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
void WKTParser::addValueIdToQueue(ValueId valueId, size_t rowIndex, bool side,
                                  std::optional<BoundingBox> boundingBox) {
  _curBatch.push_back({valueId, rowIndex, side, "", std::move(boundingBox)});

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
