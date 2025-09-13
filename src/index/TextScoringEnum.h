// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_TEXTSCORINGENUM_H
#define QLEVER_SRC_INDEX_TEXTSCORINGENUM_H

#include "global/Id.h"
#include "global/IndexTypes.h"
#include "util/HashMap.h"

// Inverted index mapping WordIndex to a list of pairs of TextRecordIndex, and
// a TermFrequency
using TermFrequency = uint32_t;
using InnerMap = ad_utility::HashMap<DocumentIndex, TermFrequency>;
using InvertedIndex = ad_utility::HashMap<WordIndex, InnerMap>;
using DocLengthMap = ad_utility::HashMap<DocumentIndex, size_t>;

namespace qlever {
enum struct TextScoringMetric { EXPLICIT, TFIDF, BM25 };

std::string getTextScoringMetricAsString(TextScoringMetric textScoringMetric);

TextScoringMetric getTextScoringMetricFromString(
    const std::string& textScoringMetricString);
}  // namespace qlever

struct TextScoringConfig {
  TextScoringMetric scoringMetric_ = TextScoringMetric::EXPLICIT;
  std::pair<float, float> bAndKParam_ = {0.75f, 1.75f};
};

#endif  // QLEVER_SRC_INDEX_TEXTSCORINGENUM_H
