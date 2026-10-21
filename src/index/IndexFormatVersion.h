//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_INDEXFORMATVERSION_H
#define QLEVER_SRC_INDEX_INDEXFORMATVERSION_H

#include <cstdint>

#include "backports/three_way_comparison.h"
#include "util/DateYearDuration.h"
#include "util/json.h"

namespace qlever {
// Return information about the last breaking change in QLever's index
// structure. If the contents in this struct differ between a built index and a
// server executable, then the index has to be rebuilt.
struct IndexFormatVersion {
  // The number of the pull request that changed the index format most recently.
  uint64_t prNumber_;
  // The date of the last breaking change of the index format.
  DateYearOrDuration date_{Date{1900, 1, 1}};

  // Conversion To JSON.
  friend void to_json(nlohmann::json& j, const IndexFormatVersion& version) {
    j["date"] = version.date_.toStringAndType().first;
    j["pull-request-number"] = version.prNumber_;
  }

  // Conversion from JSON.
  friend void from_json(const nlohmann::json& j, IndexFormatVersion& version) {
    version.prNumber_ = static_cast<uint64_t>(j["pull-request-number"]);
    version.date_ = DateYearOrDuration::parseXsdDate(std::string{j["date"]});
  }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(IndexFormatVersion, prNumber_,
                                              date_)
};

// The actual index version. Change it once the binary format of the index
// changes.
//
// NOTE: When you change it, then also assess the `previousIndexFormatVersion`
// below and the index converter that uses it (see
// `index/IndexFormatConverter.h`). That converter handles exactly one change of
// the index format, so it has to be either extended by your change or updated
// to the new pair of versions; else an index of the previous version can no
// longer be converted, but only be rebuilt. The same holds for
// `indexFormatVersionWithLatMajorGeoPoints`: an index in that format is loaded
// without conversion only as long as your change does not affect it.
inline const IndexFormatVersion& indexFormatVersion{
    3470, DateYearOrDuration{Date{2026, 9, 26}}};

// The index format that directly precedes `indexFormatVersion` above. It
// differs from the current format only in that it always encodes geo points
// in the (now deprecated) `LatMajor` encoding, and that its configuration has
// no entry for the encoding (see `ad_utility::GeoPointEncoding`). The current
// format supports that encoding as well, so an index in this format is loaded
// without any conversion.
inline const IndexFormatVersion& indexFormatVersionWithLatMajorGeoPoints{
    3159, DateYearOrDuration{Date{2026, 9, 1}}};

// The index format that the standalone index converter (see
// `index/IndexFormatConverter.h`) converts from, which is the only place that
// this constant is used. The converter converts an index in this format to the
// format `indexFormatVersionWithLatMajorGeoPoints`, which the current version
// of QLever loads as it is (see above).
inline const IndexFormatVersion& previousIndexFormatVersion{
    1572, DateYearOrDuration{Date{2024, 10, 22}}};

// Return true iff an index in the given format can be loaded by the current
// version of QLever without conversion.
inline bool isLoadableIndexFormatVersion(const IndexFormatVersion& version) {
  return version == indexFormatVersion ||
         version == indexFormatVersionWithLatMajorGeoPoints;
}
}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_INDEXFORMATVERSION_H
