// Copyright 2024 - 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/SpatialJoinParser.h"

#include "engine/SpatialJoinAlgorithms.h"

#ifdef __cpp_lib_string_view
#include <string_view>
#endif

using sjTemp::WKTParser;
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
    : _sweeper(sweeper),
      _jobs(1000),
      _thrds(numThreads),
      _bboxes(numThreads),
      _cancelled(false),
      _usePrefiltering(usePrefiltering),
      _prefilterLatLngBox(prefilterLatLngBox),
      _index(index) {
  for (size_t i = 0; i < _thrds.size(); i++) {
    _thrds[i] = std::thread(&WKTParser::processQueue, this, i);
  }
}

// _____________________________________________________________________________
WKTParser::~WKTParser() {
  // graceful shutdown of all threads, should they be still running
  _cancelled = true;

  // end event
  _jobs.add({});

  // wait for all workers to finish
  for (auto& thr : _thrds)
    if (thr.joinable()) thr.join();
}

// _____________________________________________________________________________
void WKTParser::done() {
  if (_curBatch.size()) {
    _jobs.add(std::move(_curBatch));
    _curBatch.clear();
  }

  // end event
  _jobs.add({});
  // wait for all workers to finish
  for (auto& thr : _thrds) thr.join();

  // collect bounding box of parsed geometries
  for (const auto& box : _bboxes) {
    _bbox = util::geo::extendBox(box, _bbox);
  }
}

// _____________________________________________________________________________
void WKTParser::parseLine(char* c, size_t len, size_t gid, size_t t,
                          sj::WriteBatch& batch, bool side) {
  char* idp = reinterpret_cast<char*>(strchr(c, '\t'));

  std::string id;

  if (idp) {
    *idp = 0;
    id = c;
    len -= (idp - c) + 1;
    c = idp + 1;
  } else {
    id = std::to_string(gid);
  }

  idp = reinterpret_cast<char*>(strchr(c, '\t'));

  if (idp) {
    *idp = 0;
    side = atoi(c);
    len -= (idp - c) + 1;
    c = idp + 1;
  }

  if (len > 2 && *c == '<') {
    char* end = strchr(c, ',');
    size_t subId = 0;
    c += 1;
    if (end) {
      subId = 1;
      do {
        *end = 0;
        _sweeper->add(c,
                      util::geo::I32Box({std::numeric_limits<int32_t>::min(),
                                         std::numeric_limits<int32_t>::min()},
                                        {std::numeric_limits<int32_t>::max(),
                                         std::numeric_limits<int32_t>::max()}),
                      id, subId, side, batch);
        len -= (end - c) + 1;
        c = end + 1;
      } while (len > 0 && (end = strchr(c, ',')));
    }

    c[len - 2] = 0;
    _sweeper->add(c,
                  util::geo::I32Box({std::numeric_limits<int32_t>::min(),
                                     std::numeric_limits<int32_t>::min()},
                                    {std::numeric_limits<int32_t>::max(),
                                     std::numeric_limits<int32_t>::max()}),
                  id, subId, side, batch);
  } else {
    auto wktType = getWKTType(c, const_cast<const char**>(&c));
    if (wktType == util::geo::WKTType::POINT) {
      const auto& point = pointFromWKT<int32_t>(c, 0, &projFunc);
      _bboxes[t] = util::geo::extendBox(_sweeper->add(point, id, side, batch),
                                        _bboxes[t]);
    } else if (wktType == util::geo::WKTType::MULTIPOINT) {
      const auto& mp = multiPointFromWKT<int32_t>(c, 0, &projFunc);
      if (mp.size() != 0)
        _bboxes[t] = util::geo::extendBox(_sweeper->add(mp, id, side, batch),
                                          _bboxes[t]);
    } else if (wktType == util::geo::WKTType::LINESTRING) {
      const auto& line = lineFromWKT<int32_t>(c, 0, &projFunc);
      if (line.size() > 1)
        _bboxes[t] = util::geo::extendBox(_sweeper->add(line, id, side, batch),
                                          _bboxes[t]);
    } else if (wktType == util::geo::WKTType::MULTILINESTRING) {
      const auto& ml = multiLineFromWKT<int32_t>(c, 0, &projFunc);
      _bboxes[t] =
          util::geo::extendBox(_sweeper->add(ml, id, side, batch), _bboxes[t]);
    } else if (wktType == util::geo::WKTType::POLYGON) {
      const auto& poly = polygonFromWKT<int32_t>(c, 0, &projFunc);
      if (poly.getOuter().size() > 1)
        _bboxes[t] = util::geo::extendBox(_sweeper->add(poly, id, side, batch),
                                          _bboxes[t]);
    } else if (wktType == util::geo::WKTType::MULTIPOLYGON) {
      const auto& mp = multiPolygonFromWKT<int32_t>(c, 0, &projFunc);
      if (mp.size())
        _bboxes[t] = util::geo::extendBox(_sweeper->add(mp, id, side, batch),
                                          _bboxes[t]);
    } else if (wktType == util::geo::WKTType::COLLECTION) {
      const auto& col = collectionFromWKT<int32_t>(c, 0, &projFunc);

      size_t numGeoms = 0;
      for (const auto& a : col) {
        if (a.getType() == 0) numGeoms++;
        if (a.getType() == 1) numGeoms++;
        if (a.getType() == 2) numGeoms++;
        if (a.getType() == 3) numGeoms += a.getMultiLine().size();
        if (a.getType() == 4) numGeoms += a.getMultiPolygon().size();
        if (a.getType() == 6) numGeoms += a.getMultiPoint().size();
      }

      size_t subId = numGeoms > 1 ? 1 : 0;

      for (const auto& a : col) {
        if (a.getType() == 0)
          _bboxes[t] = util::geo::extendBox(
              _sweeper->add(a.getPoint(), id, subId, side, batch), _bboxes[t]);
        if (a.getType() == 1)
          _bboxes[t] = util::geo::extendBox(
              _sweeper->add(a.getLine(), id, subId, side, batch), _bboxes[t]);
        if (a.getType() == 2)
          _bboxes[t] = util::geo::extendBox(
              _sweeper->add(a.getPolygon(), id, subId, side, batch),
              _bboxes[t]);
        if (a.getType() == 3)
          _bboxes[t] = util::geo::extendBox(
              _sweeper->add(a.getMultiLine(), id, subId, side, batch),
              _bboxes[t]);
        if (a.getType() == 4)
          _bboxes[t] = util::geo::extendBox(
              _sweeper->add(a.getMultiPolygon(), id, subId, side, batch),
              _bboxes[t]);
        if (a.getType() == 6)
          _bboxes[t] = util::geo::extendBox(
              _sweeper->add(a.getMultiPoint(), id, subId, side, batch),
              _bboxes[t]);
        subId++;
      }
    }
  }
}

// _____________________________________________________________________________
void WKTParser::processQueue(size_t t) {
  ParseBatch batch;
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
          // prefilterCounter++;
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
          // prefilterCounter++;
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
}

// _____________________________________________________________________________
void WKTParser::addValueIdToQueue(ValueId valueId, size_t id, bool side) {
  _curBatch.reserve(10000);
  _curBatch.push_back({valueId, id, side});

  if (_curBatch.size() > 10000) {
    _jobs.add(std::move(_curBatch));
    _curBatch.clear();
  }
}
