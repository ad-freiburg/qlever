// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_PARSER_PARALLELPARSEBUFFER_H
#define QLEVER_SRC_PARSER_PARALLELPARSEBUFFER_H

#include <array>
#include <future>
#include <string>
#include <vector>

#include "parser/RdfParser.h"
#include "util/Log.h"

namespace ad_utility::detail {

template <typename Parser>
CPP_requires(ParserGetBatchRequires, requires(Parser& p)(p.getBatch()));

template <typename Parser>
CPP_concept ParserGetBatch = CPP_requires_ref(ParserGetBatchRequires, Parser);

}  // namespace ad_utility::detail

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
  using Triple = std::array<std::string, 3>;

  /**
   * @brief Parse the next Triple
   * If we have already parsed the maximum number of triples specified in the
   * constructor, return std::nullopt. If the parser is exhausted and doesn't
   * deliver any more triples, call the callback and return std::nullopt. Else
   * return a optional that contains the next Triple from the parser.
   * @return
   */
  std::optional<TurtleTriple> operator()() {
    if (m_numTriplesAlreadyParsed >= m_maxNumTriples) {
      return std::nullopt;
    }
    TurtleTriple t;
    if (m_parser->getLine(t)) {
      m_numTriplesAlreadyParsed++;
      return t;
    } else {
      m_exhaustedCallback();
      return std::nullopt;
    }
  }

  CPP_member auto getBatch()
      -> CPP_ret(std::optional<std::vector<TurtleTriple>>)(
          requires ad_utility::detail::ParserGetBatch<Parser>) {
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

// A class that holds a parser for a knowledge base file (.nt, .tsv, .ttl, etc)
// and asynchronously retrieves triples from the file.
// Can be used to parallelize the parsing and processing of kb files.
// External interface is similar to a parser, so the usage makes the code simple
template <class Parser>
class ParallelParseBuffer {
 public:
  // Parse from the file at filename.
  // A batch of bufferSize triples is always parsed in parallel
  // bigger bufferSizes will increase memory usage whereas smaller sizes might
  // be inefficient.
  ParallelParseBuffer(size_t bufferSize, const std::string& filename)
      : _bufferSize(bufferSize), _p(filename) {
    // parse the initial batch, before this we cannot do anything
    std::tie(_isParserValid, _buffer) = parseBatch();

    // already submit the next batch for parallel computation
    if (_isParserValid) {
      _fut = std::async(std::launch::async,
                        &ParallelParseBuffer<Parser>::parseBatch, this);
    }
  }

  // Retrieve and return a the next triple from the internal buffer.
  // If the buffer is exhausted blocks
  // until the (asynchronous) call to parseBatch has finished. A nullopt
  // the parser has completely parsed the file.
  std::optional<std::array<std::string, 3>> getTriple() {
    // Return our triple in the order the parser handles them to us.
    // Makes debugging easier.
    if (_buffer.size() == _bufferPosition && _isParserValid) {
      // we have to wait for the next parallel batch to be completed.
      std::tie(_isParserValid, _buffer) = _fut.get();
      _bufferPosition = 0;
      if (_isParserValid) {
        // if possible, directly submit the next parsing job
        _fut = std::async(std::launch::async,
                          &ParallelParseBuffer<Parser>::parseBatch, this);
      }
    }

    // we now should have some triples in our buffer
    if (_bufferPosition < _buffer.size()) {
      return _buffer[_bufferPosition++];
    } else {
      // we can only reach this if the buffer is exhausted and there is nothing
      // more to parse
      return std::nullopt;
    }
  }

 private:
  const size_t _bufferSize;
  // needed for the ringbuffer-style access
  size_t _bufferPosition = 0;
  Parser _p;
  // becomes false when the parser is done. In this case we still have to
  // empty our buffer
  bool _isParserValid = true;
  std::vector<std::array<std::string, 3>> _buffer;
  // this future handles the asynchronous parser calls
  std::future<std::pair<bool, std::vector<std::array<std::string, 3>>>> _fut;

  // this function extracts bufferSize_ many triples from the parser.
  // If the bool argument is false, the parser is exhausted and further calls
  // to parseBatch are useless. In this case we probably still have some triples
  // that were parsed before the parser was done, so we still have to consider
  // these.
  std::pair<bool, std::vector<std::array<std::string, 3>>> parseBatch() {
    LOG(TRACE) << "Parsing next batch in parallel" << std::endl;
    std::vector<std::array<std::string, 3>> buf;
    // for small knowledge bases on small systems that fit in one
    // batch (e.g. during tests) the reserve may fail which is not bad in this
    // case
    try {
      buf.reserve(_bufferSize);
    } catch (const std::bad_alloc&) {
      buf = std::vector<std::array<std::string, 3>>();
    }
    while (buf.size() < _bufferSize) {
      buf.emplace_back();
      if (!_p.getLine(buf.back())) {
        buf.pop_back();
        return {false, std::move(buf)};
      }
      if (buf.size() % 10000000 == 0) {
        LOG(INFO) << "Parsed " << buf.size() << " triples." << std::endl;
      }
    }
    return {true, std::move(buf)};
  }
};

#endif  // QLEVER_SRC_PARSER_PARALLELPARSEBUFFER_H
