// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/SpatialJoinParser.h"

#include <iterator>
#include <range/v3/numeric/accumulate.hpp>
#include <tuple>

#include "engine/spatialJoinAlgorithms/LibspatialjoinAlgorithm.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/ExceptionHandling.h"

namespace ad_utility::detail::parallel_wkt_parser {

// _____________________________________________________________________________
WKTParser::WKTParser(sj::GeometryStore* store, sj::PreparedGeomMetaCb metaCb,
                     size_t numThreads, bool usePrefiltering,
                     const std::optional<::util::geo::DBox>& prefilterLatLngBox,
                     const Index& index, bool usePersistedGeometries)
    : sj::WKTParserBase<SpatialJoinParseJob>(store, std::move(metaCb),
                                             numThreads),
      _numSkipped(numThreads),
      _numParsed(numThreads),
      _usePersistedGeometries(usePersistedGeometries),
      _storedRows(numThreads),
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
        // An empty batch is the end event of the job queue (see `done()`). The
        // queue never removes it, so one end event stops all threads.
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
std::vector<StoredGeometryRow> WKTParser::takeStoredRows() {
  std::vector<StoredGeometryRow> result;
  for (auto& rows : _storedRows) {
    ql::ranges::move(rows, std::back_inserter(result));
    rows.clear();
  }
  ql::ranges::sort(result, [](const auto& a, const auto& b) {
    return std::tie(a.position_, a.row_) < std::tie(b.position_, b.row_);
  });
  return result;
}

// _____________________________________________________________________________
std::optional<StoredGeometryRow> WKTParser::storedGeometryRow(
    const SpatialJoinParseJob& job) const {
  if (!_usePersistedGeometries) {
    return std::nullopt;
  }
  auto vocabIndex = job.valueId.getVocabIndex();
  auto info = _index.getVocab().getGeoInfo(vocabIndex);
  if (!info.has_value() || !info->getGeometryStoreId().has_value()) {
    return std::nullopt;
  }
  auto position = _index.getVocab().getGeoVocabularyPosition(vocabIndex);
  AD_CORRECTNESS_CHECK(position.has_value());

  // The corners of the bounding box of a `GeometryInfo` are rounded to the
  // nearest `GeoPoint` (a step of less than `2e-7` degrees), so pad it to be
  // sure that it contains the geometry.
  constexpr double padding = 1e-6;
  auto box = ad_utility::detail::boundingBoxToUtilBox(info->getBoundingBox());
  box = ::util::geo::DBox{{box.getLowerLeft().getX() - padding,
                           box.getLowerLeft().getY() - padding},
                          {box.getUpperRight().getX() + padding,
                           box.getUpperRight().getY() + padding}};
  return StoredGeometryRow{
      position.value(), job.line, info->getGeometryStoreId().value(),
      ad_utility::detail::projectDoubleLatLngToInt32WebMerc(box)};
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
        // A geometry from the persisted store is collected instead of parsed.
        // Its bounding box serves for the prefilter (no further disk access).
        if (auto stored = storedGeometryRow(job); stored.has_value()) {
          if (_prefilterLatLngBox.has_value() &&
              !::util::geo::intersects(
                  _prefilterLatLngBox.value(),
                  ad_utility::detail::projectInt32WebMercToDoubleLatLng(
                      stored->boundingBox_))) {
            prefilterCounter++;
            continue;
          }
          addBox(_store->getPaddedBoundingBox(stored->boundingBox_), t);
          _storedRows[t].push_back(std::move(stored.value()));
          parseCounter++;
          continue;
        }

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
        parseWKTGeometry(job.wkt.c_str(), std::to_string(job.line), t, w, true);
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
        addBox(_store->add(addPoint, std::to_string(job.line), w), t);
        parseCounter++;
      } else if (dt == Datatype::LocalVocabIndex) {
        // `LocalVocabEntry` has to be parsed in any case: we have no
        // information except the string.
        // Invalid WKT is skipped as above.
        const auto& literalOrIri = *job.valueId.getLocalVocabIndex();
        if (literalOrIri.isLiteral()) {
          job.wkt = literalOrIri.getLiteral().getContent();
          parseWKTGeometry(job.wkt.c_str(), std::to_string(job.line), t, w,
                           true);
          parseCounter++;
        }
      }
    }

    _store->addBatch(w, t, _metaCb);
  }

  _numSkipped[t] = prefilterCounter;
  _numParsed[t] = parseCounter;
}

// _____________________________________________________________________________
void WKTParser::addValueIdToQueue(ValueId valueId, size_t rowIndex,
                                  std::optional<BoundingBox> boundingBox) {
  _curBatch.push_back({valueId, rowIndex, "", std::move(boundingBox)});

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
