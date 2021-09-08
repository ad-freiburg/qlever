// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#include "./TurtleParser.h"

#include <string.h>

#include "../util/TaskQueue.h"
#include "./RdfEscaping.h"

// _______________________________________________________________
template <class T>
bool TurtleParser<T>::statement() {
  _tok.skipWhitespaceAndComments();
  return directive() || (triples() && skip<TurtleTokenId::Dot>());
}

// ______________________________________________________________
template <class T>
bool TurtleParser<T>::directive() {
  return prefixID() || base() || sparqlPrefix() || sparqlBase();
}

// ________________________________________________________________
template <class T>
bool TurtleParser<T>::prefixID() {
  if (skip<TurtleTokenId::TurtlePrefix>()) {
    if (check(pnameNS()) && check(iriref()) &&
        check(skip<TurtleTokenId::Dot>())) {
      // strip  the angled brackes <bla> -> bla
      _prefixMap[_activePrefix] =
          _lastParseResult.substr(1, _lastParseResult.size() - 2);
      return true;
    } else {
      raise("prefixID");
    }
  } else {
    return false;
  }
}

// ________________________________________________________________
template <class T>
bool TurtleParser<T>::base() {
  if (skip<TurtleTokenId::TurtleBase>()) {
    if (iriref()) {
      _baseIRI = _lastParseResult;
      return true;
    } else {
      raise("base");
    }
  } else {
    return false;
  }
}

// ________________________________________________________________
template <class T>
bool TurtleParser<T>::sparqlPrefix() {
  if (skip<TurtleTokenId::SparqlPrefix>()) {
    if (pnameNS() && iriref()) {
      _prefixMap[_activePrefix] = _lastParseResult;
      return true;
    } else {
      raise("sparqlPrefix");
    }
  } else {
    return false;
  }
}

// ________________________________________________________________
template <class T>
bool TurtleParser<T>::sparqlBase() {
  if (skip<TurtleTokenId::SparqlBase>()) {
    if (iriref()) {
      _baseIRI = _lastParseResult;
      return true;
    } else {
      raise("sparqlBase");
    }
  } else {
    return false;
  }
}

// ________________________________________________
template <class T>
bool TurtleParser<T>::triples() {
  if (subject()) {
    if (predicateObjectList()) {
      return true;
    } else {
      raise("triples");
    }
  } else {
    if (blankNodePropertyList()) {
      predicateObjectList();
      return true;
    } else {
      // TODO: do we need to throw here
      return false;
    }
  }
}

// __________________________________________________
template <class T>
bool TurtleParser<T>::predicateObjectList() {
  if (verb() && check(objectList())) {
    while (skip<TurtleTokenId::Semicolon>()) {
      if (verb() && check(objectList())) {
        continue;
      }
    }
    return true;
  } else {
    return false;
  }
}

// _____________________________________________________
template <class T>
bool TurtleParser<T>::objectList() {
  if (object()) {
    while (skip<TurtleTokenId::Comma>() && check(object())) {
      continue;
    }
    return true;
  } else {
    return false;
  }
}

// ______________________________________________________
template <class T>
bool TurtleParser<T>::verb() {
  return predicateSpecialA() || predicate();
}

// ___________________________________________________________________
template <class T>
bool TurtleParser<T>::predicateSpecialA() {
  _tok.skipWhitespaceAndComments();
  if (auto [success, word] = _tok.template getNextToken<TurtleTokenId::A>();
      success) {
    (void)word;
    _activePredicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"s;
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
template <class T>
bool TurtleParser<T>::subject() {
  if (blankNode() || iri() || collection()) {
    _activeSubject = _lastParseResult;
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
template <class T>
bool TurtleParser<T>::predicate() {
  if (iri()) {
    _activePredicate = _lastParseResult;
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
template <class T>
bool TurtleParser<T>::object() {
  // these produce a single object that becomes part of a triple
  // check blank Node first because _: also could look like a prefix
  if (blankNode() || literal() || iri()) {
    emitTriple();
    return true;
  } else if (collection() || blankNodePropertyList()) {
    // these have a more complex logic and produce their own triples
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
template <class T>
bool TurtleParser<T>::literal() {
  return (rdfLiteral() || numericLiteral() || booleanLiteral());
}

// _____________________________________________________________________
template <class T>
bool TurtleParser<T>::blankNodePropertyList() {
  if (!skip<TurtleTokenId::OpenSquared>()) {
    return false;
  }
  // save subject and predicate
  string savedSubject = _activeSubject;
  string savedPredicate = _activePredicate;
  // new triple with blank node as object
  string blank = createAnonNode();
  _lastParseResult = blank;
  emitTriple();
  // the following triples have the blank node as subject
  _activeSubject = blank;
  check(predicateObjectList());
  check(skip<TurtleTokenId::CloseSquared>());
  // restore subject and predicate
  _activeSubject = savedSubject;
  _activePredicate = savedPredicate;
  return true;
}

// _____________________________________________________________________
template <class T>
bool TurtleParser<T>::collection() {
  if (!skip<TurtleTokenId::OpenRound>()) {
    return false;
  }
  raise("We do not know how to handle collections in QLever yet\n");
  // TODO<joka921> understand collections
}

// ______________________________________________________________________
template <class T>
bool TurtleParser<T>::numericLiteral() {
  return integer() || decimal() || doubleParse();
}

// ______________________________________________________________________
template <class T>
bool TurtleParser<T>::rdfLiteral() {
  if (!stringParse()) {
    return false;
  }
  auto literalString = _lastParseResult =
      RdfEscaping::normalizeRDFLiteral(_lastParseResult);
  if (langtag()) {
    _lastParseResult = literalString + _lastParseResult;
    return true;
    // TODO<joka921> this allows spaces here since the ^^ is unique in the
    // sparql syntax. is this correct?
  } else if (skip<TurtleTokenId::DoubleCircumflex>() && check(iri())) {
    _lastParseResult = literalString + "^^" + _lastParseResult;
    return true;
  } else {
    // it is okay to neither have a langtag nor a xsd datatype
    return true;
  }
}

// ______________________________________________________________________
template <class T>
bool TurtleParser<T>::booleanLiteral() {
  return parseTerminal<TurtleTokenId::True>() ||
         parseTerminal<TurtleTokenId::False>();
}

// ______________________________________________________________________
template <class T>
bool TurtleParser<T>::stringParse() {
  // manually parse strings for efficiency
  auto view = _tok.view();
  size_t startPos = 0;
  size_t endPos = 1;
  std::array<string, 4> quotes{R"(""")", R"(''')", "\"", "\'"};
  bool foundString = false;
  for (const auto& q : quotes) {
    if (ad_utility::startsWith(view, q)) {
      foundString = true;
      startPos = q.size();
      endPos = view.find(q, startPos);
      while (endPos != string::npos) {
        if (view[endPos - 1] == '\\') {
          size_t numBackslash = 1;
          auto slashPos = endPos - 2;
          while (view[slashPos] == '\\') {
            slashPos--;
            numBackslash++;
          }
          if (numBackslash % 2 == 0) {
            // even number of backslashes means that the quote we found has not
            // been escaped
            break;
          }
          endPos = view.find(q, endPos + 1);
        } else {
          // no backslash before " , the string has definitely ended
          break;
        }
      }
      break;
    }
  }
  if (!foundString) {
    return false;
  }
  if (endPos == string::npos) {
    raise("unterminated string Literal");
  }
  // also include the quotation marks in the word
  // TODO <joka921> how do we have to translate multiline strings for QLever?
  _lastParseResult = view.substr(0, endPos + startPos);
  _tok.remove_prefix(endPos + startPos);
  return true;
}

// ______________________________________________________________________
template <class T>
bool TurtleParser<T>::iri() {
  // irirefs always start with "<", prefixedNames never, so the lookahead always
  // works
  return iriref() || prefixedName();
}

// _____________________________________________________________________
template <class T>
bool TurtleParser<T>::prefixedName() {
  if constexpr (UseRelaxedParsing) {
    if (!(pnameLnRelaxed() || pnameNS())) {
      return false;
    }
  } else {
    if (!pnameNS()) {
      return false;
    }
    parseTerminal<TurtleTokenId::PnLocal, false>();
  }
  _lastParseResult = '<' + expandPrefix(_activePrefix) +
                     RdfEscaping::unescapePrefixedIri(_lastParseResult) + '>';
  return true;
}

// _____________________________________________________________________
template <class T>
bool TurtleParser<T>::blankNode() {
  return blankNodeLabel() || anon();
}

// _______________________________________________________________________
template <class T>
template <TurtleTokenId terminal, bool SkipWhitespaceBefore>
bool TurtleParser<T>::parseTerminal() {
  if constexpr (SkipWhitespaceBefore) {
    _tok.skipWhitespaceAndComments();
  }
  auto [success, word] = _tok.template getNextToken<terminal>();
  if (success) {
    _lastParseResult = word;
    return true;
  } else {
    return false;
  }
  return false;
}

// ________________________________________________________________________
template <class T>
bool TurtleParser<T>::blankNodeLabel() {
  return parseTerminal<TurtleTokenId::BlankNodeLabel>();
}

// __________________________________________________________________________
template <class T>
bool TurtleParser<T>::pnameNS() {
  if (parseTerminal<TurtleTokenId::PnameNS>()) {
    // this also includes a ":" which we do not need, hence the "-1"
    _activePrefix = _lastParseResult.substr(0, _lastParseResult.size() - 1);
    _lastParseResult = "";
    return true;
  } else {
    return false;
  }
}

// ________________________________________________________________________
template <class T>
bool TurtleParser<T>::pnameLnRelaxed() {
  // relaxed parsing, only works if the greedy parsing of the ":"
  // is ok
  _tok.skipWhitespaceAndComments();
  auto view = _tok.view();
  auto pos = view.find(':');
  if (pos == string::npos) {
    return false;
  }
  // these can also be part of a collection etc.
  // find any character that can end a pnameLn when assuming that no
  // escape sequences were used
  auto posEnd = view.find_first_of(" \n,;", pos);
  if (posEnd == string::npos) {
    // make tests work
    posEnd = view.size();
  }
  // TODO: is it allowed to have no space between triples and the dots? in
  // this case we have to check something here
  _activePrefix = view.substr(0, pos);
  _lastParseResult = view.substr(pos + 1, posEnd - (pos + 1));
  // we do not remove the whitespace or the ,; since they are needed
  _tok.remove_prefix(posEnd);
  return true;
}

// _____________________________________________________________________
template <class T>
bool TurtleParser<T>::iriref() {
  if constexpr (UseRelaxedParsing) {
    // manually check if the input starts with "<" and then find the next ">"
    // this might accept invalid irirefs but is faster than checking the
    // complete regexes
    _tok.skipWhitespaceAndComments();
    auto view = _tok.view();
    if (ad_utility::startsWith(view, "<")) {
      auto endPos = view.find_first_of("> \n");
      if (endPos == string::npos || view[endPos] != '>') {
        raise("Iriref");
      } else {
        _tok.remove_prefix(endPos + 1);
        _lastParseResult =
            RdfEscaping::unescapeIriref(view.substr(0, endPos + 1));
        return true;
      }
    } else {
      return false;
    }
  } else {
    if (!parseTerminal<TurtleTokenId::Iriref>()) {
      return false;
    }
    _lastParseResult = RdfEscaping::unescapeIriref(_lastParseResult);
    return true;
  }
}

// ______________________________________________________________________
template <class T>
typename TurtleStreamParser<T>::TurtleParserBackupState
TurtleStreamParser<T>::backupState() const {
  TurtleParserBackupState b;
  b._numBlankNodes = this->_numBlankNodes;
  b._numTriples = this->_triples.size();
  b._tokenizerPosition = this->_tok.data().begin();
  b._tokenizerSize = this->_tok.data().size();
  return b;
}

// _______________________________________________________________
template <class T>
bool TurtleStreamParser<T>::resetStateAndRead(
    TurtleStreamParser::TurtleParserBackupState* bPtr) {
  auto& b = *bPtr;
  auto nextBytesOpt = _fileBuffer->getNextBlock();
  if (!nextBytesOpt || nextBytesOpt.value().empty()) {
    // there are no more decompressed bytes, just continue with what we've got
    // do not alter any internal state.
    return false;
  }

  auto nextBytes = std::move(nextBytesOpt.value());

  // return to the state of the last backup
  this->_numBlankNodes = b._numBlankNodes;
  AD_CHECK(this->_triples.size() >= b._numTriples);
  this->_triples.resize(b._numTriples);
  this->_tok.reset(b._tokenizerPosition, b._tokenizerSize);

  std::vector<char> buf;

  // Used for a more informative error message when a parse error occurs (see
  // function "raise").
  _numBytesBeforeCurrentBatch += _byteVec.size() - _tok.data().size();
  buf.resize(_tok.data().size() + nextBytes.size());
  memcpy(buf.data(), _tok.data().begin(), _tok.data().size());
  memcpy(buf.data() + _tok.data().size(), nextBytes.data(), nextBytes.size());
  _byteVec = std::move(buf);
  _tok.reset(_byteVec.data(), _byteVec.size());

  LOG(TRACE) << "Succesfully decompressed next batch of " << nextBytes.size()
             << " << bytes to parser\n";

  // repair the backup state, its pointers might have changed due to
  // reallocation
  b = backupState();
  return true;
}

// __________________________________________________________________________
template <class T>
void TurtleMmapParser<T>::initialize(const string& filename) {
  unmapFile();
  this->clear();
  ad_utility::File f(filename.c_str(), "r");
  size_t size = f.sizeOfFile();
  LOG(INFO) << "mapping " << size << " bytes" << std::endl;
  const int fd = f.getFileDescriptor();
  void* ptr = mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
  AD_CHECK(ptr != MAP_FAILED);
  f.close();
  _dataSize = size;
  _data = static_cast<char*>(ptr);
  // set the tokenizers input to the complete mmap range
  _tok.reset(_data, _dataSize);
}

template <class T>
bool TurtleMmapParser<T>::getLine(std::array<string, 3>* triple) {
  if (_triples.empty()) {
    // always try to parse a batch of triples at once to make up for the
    // relatively expensive backup calls.
    while (_triples.size() < PARSER_MIN_TRIPLES_AT_ONCE &&
           !_isParserExhausted) {
      if (this->statement()) {
        // we cannot parse anymore from an mmaped file but there was no
        // error. check if we are just at the end of the input, otherwise
        // inform.
        _tok.skipWhitespaceAndComments();
        auto d = _tok.view();
        if (!d.empty()) {
          LOG(INFO) << "Parsing of line has Failed, but parseInput is not "
                       "yet exhausted. Remaining bytes: "
                    << d.size() << '\n';
          auto s = std::min(size_t(1000), size_t(d.size()));
          LOG(INFO) << "Logging first 1000 unparsed characters\n";
          LOG(INFO) << std::string_view(d.data(), s) << std::endl;
        }
        _isParserExhausted = true;
        break;
      }
    }
  }
  // if we have a triple now we can return it, else we are done parsing.
  if (_triples.empty()) {
    return false;
  }

  // we now have at least one triple, return it.
  *triple = _triples.back();
  _triples.pop_back();
  return true;
}

template <class T>
void TurtleStreamParser<T>::initialize(const string& filename) {
  this->clear();
  _fileBuffer = std::make_unique<ParallelFileBuffer>(_bufferSize);
  _fileBuffer->open(filename);
  _byteVec.resize(_bufferSize);
  // decompress the first block and initialize Tokenizer
  if (auto res = _fileBuffer->getNextBlock(); res) {
    _byteVec = std::move(res.value());
    _tok.reset(_byteVec.data(), _byteVec.size());
  } else {
    LOG(WARN)
        << "The input stream for the turtle parser seems to contain no data!\n";
  }
}

template <class T>
bool TurtleStreamParser<T>::getLine(std::array<string, 3>* triple) {
  if (_triples.empty()) {
    // if parsing the line fails because our buffer ends before the end of
    // the next statement we need to be able to recover
    TurtleParserBackupState b = backupState();
    // always try to parse a batch of triples at once to make up for the
    // relatively expensive backup calls.
    while (_triples.size() < PARSER_MIN_TRIPLES_AT_ONCE &&
           !_isParserExhausted) {
      bool parsedStatement;
      bool exceptionThrown = false;
      typename TurtleParser<T>::ParseException ex;
      // If this buffer reads from an mmaped file, then exceptions are
      // immediately rethrown. If we are reading from a stream in chunks of
      // bytes, we can try again with a larger buffer.
      try {
        // variable parsedStatement will be true iff a statement can
        // successfully be parsed
        parsedStatement = this->statement();
      } catch (const typename TurtleParser<T>::ParseException& p) {
        parsedStatement = false;
        exceptionThrown = true;
        ex = p;
      }

      if (!parsedStatement) {
        // we read chunks of memories in a buffered way
        // try to parse with a larger buffer and repeat the reading process
        // (maybe the failure was due to statements crossing our block).
        if (resetStateAndRead(&b)) {
          // we have succesfully extended our buffer
          if (_byteVec.size() > BZIP2_MAX_TOTAL_BUFFER_SIZE) {
            auto d = _tok.view();
            LOG(ERROR) << "Could not parse " << PARSER_MIN_TRIPLES_AT_ONCE
                       << " Within " << (BZIP2_MAX_TOTAL_BUFFER_SIZE >> 10)
                       << "MB of Turtle input\n";
            LOG(ERROR) << "If you really have Turtle input with such a "
                          "long structure please recompile with adjusted "
                          "constants in ConstantsIndexCreation.h or "
                          "decompress your file and "
                          "use --file-format mmap\n";
            auto s = std::min(size_t(1000), size_t(d.size()));
            LOG(INFO) << "Logging first 1000 unparsed characters\n";
            LOG(INFO) << std::string_view(d.data(), s);
            if (exceptionThrown) {
              throw ex;

            } else {
              this->raise(
                  "Too many bytes parsed without finishing a turtle "
                  "statement");
            }
          }
          // we have reset our state to a safe position and now have more
          // bytes to try, so just go to the next iterations
          continue;
        } else {
          // there are no more bytes in the buffer
          if (exceptionThrown) {
            throw ex;
          } else {
            // we are at the end of an input stream without an exception
            // the input is exhausted, but we still may retrieve
            // triples parsed so far, check if we have indeed parsed through
            // the complete input
            _tok.skipWhitespaceAndComments();
            auto d = _tok.view();
            if (!d.empty()) {
              LOG(INFO) << "Parsing of line has Failed, but parseInput is not "
                           "yet exhausted. Remaining bytes: "
                        << d.size() << '\n';
              auto s = std::min(size_t(1000), size_t(d.size()));
              LOG(INFO) << "Logging first 1000 unparsed characters\n";
              LOG(INFO) << std::string_view(d.data(), s);
            }
            _isParserExhausted = true;
            break;
          }
        }
      }
    }
  }

  // if we have a triple now we can return it, else we are done parsing.
  if (_triples.empty()) {
    return false;
  }

  // we now have at least one triple, return it.
  *triple = _triples.back();
  _triples.pop_back();
  return true;
}

template <typename Tokenizer_T>
void TurtleParallelParser<Tokenizer_T>::initialize(const string& filename) {
  _fileBuffer.open(filename);
  auto batch = _fileBuffer.getNextBlock();
  if (!batch) {
    throw std::runtime_error("Could not read from the input file or stream");
  }
  TurtleStringParser<Tokenizer_T> declarationParser{};
  declarationParser.setInputStream(*batch);
  while (declarationParser.parseDirectiveManually()) {
  }
  this->_prefixMap = std::move(declarationParser.getPrefixMap());
  auto remainder = declarationParser.getUnparsedRemainder();
  if (remainder.empty()) {
    declarationParser.raiseManually(
        "The prologue (prefix/base declarations) seems to be longer than the "
        "parser's block size. This should never happen, please report this");
  }
  _remainingBatchFromInitialization.clear();
  _remainingBatchFromInitialization.reserve(remainder.size());
  std::copy(remainder.begin(), remainder.end(),
            std::back_inserter(_remainingBatchFromInitialization));

  // This lambda fetches all the unparsed blocks of triples from the input
  // file and feeds them to the parallel parsers.
  auto feedBatches = [&, first = true, parsePosition = 0ull]() mutable {
    decltype(_remainingBatchFromInitialization) inputBatch;
    while (true) {
      if (first) {
        inputBatch = std::move(_remainingBatchFromInitialization);
        first = false;
      } else {
        auto nextOptional = _fileBuffer.getNextBlock();
        if (!nextOptional) {
          // Wait until everything has been parsed.
          parallelParser.finish();
          // Wait until all the parsed triples have been picked up.
          tripleCollector.finish();
          return;
        }
        inputBatch = std::move(nextOptional.value());
      }
      auto batchSize = inputBatch.size();
      auto parseBatch = [this, parsePosition, batch = std::move(inputBatch)]() {
        TurtleStringParser<Tokenizer_T> parser;
        parser._prefixMap = this->_prefixMap;
        parser.setPositionOffset(parsePosition);
        parser.setInputStream(std::move(batch));
        // TODO: raise error message if a prefix parsing fails;
        // TODO: handle exceptions in threads;
        std::vector<Triple> triples = parser.parseAndReturnAllTriples();

        tripleCollector.push([triples = std::move(triples), this]() {
          _triples = std::move(triples);
        });
      };
      parsePosition += batchSize;
      parallelParser.push(parseBatch);
    }
  };

  _parseFuture = std::async(std::launch::async, feedBatches);
}

template <class T>
bool TurtleParallelParser<T>::getLine(std::array<string, 3>* triple) {
  // If the current batch is out of _triples get the next batch of triples.
  // We need a while loop instead of a simple if in case there is a batch that
  // contains no triples. (Theoretically this might happen, and it is safer this
  // way)
  while (_triples.empty()) {
    auto optionalTripleTask = tripleCollector.popManually();
    if (!optionalTripleTask) {
      // Everything has been parsed
      return false;
    }
    // OptionalTripleTask fills the _triples vector
    (*optionalTripleTask)();
  }

  // we now have at least one triple, return it.
  *triple = std::move(_triples.back());
  _triples.pop_back();
  return true;
}

template <class T>
std::optional<std::vector<std::array<string, 3>>>
TurtleParallelParser<T>::getBatch() {
  // we need a while in case there is a batch that contains no triples
  // (this should be rare, // TODO warn about this
  while (_triples.empty()) {
    auto optionalTripleTask = tripleCollector.popManually();
    if (!optionalTripleTask) {
      // everything has been parsed
      return std::nullopt;
    }
    (*optionalTripleTask)();
  }

  return std::move(_triples);
}

// Explicit instantiations
template class TurtleParser<Tokenizer>;
template class TurtleParser<TokenizerCtre>;
template class TurtleStreamParser<Tokenizer>;
template class TurtleStreamParser<TokenizerCtre>;
template class TurtleMmapParser<Tokenizer>;
template class TurtleMmapParser<TokenizerCtre>;
template class TurtleParallelParser<Tokenizer>;
template class TurtleParallelParser<TokenizerCtre>;
