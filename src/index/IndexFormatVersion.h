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
// longer be converted, but only be rebuilt.
inline const IndexFormatVersion& indexFormatVersion{
    3159, DateYearOrDuration{Date{2026, 9, 1}}};

// The index version that directly precedes `indexFormatVersion` above. An index
// with exactly this version can be converted to the current version by the
// standalone index converter (see `index/IndexFormatConverter.h`), which is the
// only place that this constant is used.
inline const IndexFormatVersion& previousIndexFormatVersion{
    1572, DateYearOrDuration{Date{2024, 10, 22}}};
}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_INDEXFORMATVERSION_H
