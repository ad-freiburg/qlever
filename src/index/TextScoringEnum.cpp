// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/TextScoringEnum.h"

// ____________________________________________________________________________
std::string getTextScoringMetricAsString(TextScoringMetric textScoringMetric) {
  switch (textScoringMetric) {
    case TextScoringMetric::EXPLICIT:
      return "explicit";
      break;
    case TextScoringMetric::TFIDF:
      return "tf-idf";
      break;
    case TextScoringMetric::BM25:
      return "bm25";
      break;
    default:
      return "count";
      break;
  }
}

// ____________________________________________________________________________
TextScoringMetric getTextScoringMetricFromString(
    std::string textScoringMetricString) {
  if (textScoringMetricString == "tf-idf") {
    return TextScoringMetric::TFIDF;
  } else if (textScoringMetricString == "bm25") {
    return TextScoringMetric::BM25;
  } else {
    return TextScoringMetric::EXPLICIT;
  }
}
