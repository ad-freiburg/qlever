// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2021 Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//   2022 Julian Mundhahs (mundhahj@tf.informatik.uni-freiburg.de)
//   2023 Andre Schlegel (schlegea@informatik.uni-freiburg.de)
#pragma once

#include "ParserRuleContext.h"
#include "util/GenerateExceptionMetadata.h"
#include "util/ParseException.h"

ExceptionMetadata generateMetadata(const antlr4::ParserRuleContext* ctx);

ExceptionMetadata generateMetadata(antlr4::Recognizer* recognizer,
                                   antlr4::Token* offendingToken, size_t line,
                                   size_t charPositionInLine);
