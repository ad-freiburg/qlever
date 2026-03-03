// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2021 Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//   2022 Julian Mundhahs (mundhahj@tf.informatik.uni-freiburg.de)
//   2023 Andre Schlegel (schlegea@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_ANTLR_GENERATEANTLREXCEPTIONMETADATA_H
#define QLEVER_SRC_UTIL_ANTLR_GENERATEANTLREXCEPTIONMETADATA_H

#include "ParserRuleContext.h"
#include "util/ParseException.h"

namespace ad_utility::antlr_utility {
ExceptionMetadata generateAntlrExceptionMetadata(
    const antlr4::ParserRuleContext* ctx);

ExceptionMetadata generateAntlrExceptionMetadata(
    antlr4::Recognizer* recognizer, const antlr4::Token* offendingToken,
    size_t line, size_t charPositionInLine);
}  // namespace ad_utility::antlr_utility

#endif  // QLEVER_SRC_UTIL_ANTLR_GENERATEANTLREXCEPTIONMETADATA_H
