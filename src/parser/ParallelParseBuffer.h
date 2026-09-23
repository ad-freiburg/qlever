// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_PARSER_PARALLELPARSEBUFFER_H
#define QLEVER_SRC_PARSER_PARALLELPARSEBUFFER_H

#include <memory>
#include <optional>
#include <vector>

#include "parser/RdfParser.h"

/**
 * A wrapper to make the different Parsers interfaces compatible with the
 * parallel pipeline
 *
 * @tparam Parser A knowledge Base Parser Type
 * @tparam ExhaustedCallback Is called when the Parser delivered no more
 * triples.
 */
template <typename Parser, typename ExhaustedCallback>
class ParserBatcher {
 public:
  /// construct from a Parser, the maximum Number of triples to parse, and the
  /// callback.
  ParserBatcher(std::shared_ptr<Parser> p, size_t maxNumTriples,
                ExhaustedCallback c)
      : m_parser(std::move(p)),
        m_maxNumTriples(maxNumTriples),
        m_exhaustedCallback(c) {}

  // Parse the next batch of triples. If we have already parsed the maximum
  // number of triples specified in the constructor, return `std::nullopt`. If
  // the parser is exhausted and doesn't deliver any more triples, call the
  // callback and return `std::nullopt`. Else return the next batch of triples
  // from the parser.
  std::optional<std::vector<TurtleTriple>> getBatch() {
    if (m_numTriplesAlreadyParsed >= m_maxNumTriples) {
      return std::nullopt;
    }
    auto opt = m_parser->getBatch();
    if (!opt) {
      m_exhaustedCallback();
    } else {
      m_numTriplesAlreadyParsed += opt->size();
    }
    return opt;
  }

  std::shared_ptr<Parser> m_parser;
  size_t m_maxNumTriples;
  size_t m_numTriplesAlreadyParsed = 0;
  ExhaustedCallback m_exhaustedCallback;
};

#endif  // QLEVER_SRC_PARSER_PARALLELPARSEBUFFER_H
