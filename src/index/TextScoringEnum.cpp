// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/TextScoringEnum.h"

// ____________________________________________________________________________
std::string getTextScoringMetricAsString(TextScoringMetric textScoringMetric) {
  switch (textScoringMetric) {
    using enum TextScoringMetric;
    case EXPLICIT:
      return "explicit";
    case TFIDF:
      return "tf-idf";
    case BM25:
      return "bm25";
    default:
      return "explicit";
  }
}

// ____________________________________________________________________________
TextScoringMetric getTextScoringMetricFromString(
    const std::string& textScoringMetricString) {
  using enum TextScoringMetric;
  if (textScoringMetricString == "tf-idf") {
    return TFIDF;
  }
  if (textScoringMetricString == "bm25") {
    return BM25;
  }
  return EXPLICIT;
}
