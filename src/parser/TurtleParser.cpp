// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#include "./TurtleParser.h"
#include <string.h>

// _______________________________________________________________
template <class T>
bool TurtleParser<T>::statement() {
  _tok.skipWhitespaceAndComments();
  return directive() || (triples() && skip<TokId::Dot>());
}

// ______________________________________________________________
template <class T>
bool TurtleParser<T>::directive() {
  return prefixID() || base() || sparqlPrefix() || sparqlBase();
}

// ________________________________________________________________
template <class T>
bool TurtleParser<T>::prefixID() {
  if (skip<TokId::TurtlePrefix>()) {
    if (check(pnameNS()) && check(iriref()) && check(skip<TokId::Dot>())) {
      // strip  the angled brackes <bla> -> bla
      _prefixMap[_activePrefix] =
          _lastParseResult.substr(1, _lastParseResult.size() - 2);
      return true;
    } else {
      throw TurtleParser<T>::ParseException("prefixID");
    }
  } else {
    return false;
  }
}

// ________________________________________________________________
template <class T>
bool TurtleParser<T>::base() {
  if (skip<TokId::TurtleBase>()) {
    if (iriref()) {
      _baseIRI = _lastParseResult;
      return true;
    } else {
      throw ParseException("base");
    }
  } else {
    return false;
  }
}

// ________________________________________________________________
template <class T>
bool TurtleParser<T>::sparqlPrefix() {
  if (skip<TokId::SparqlPrefix>()) {
    if (pnameNS() && iriref()) {
      _prefixMap[_activePrefix] = _lastParseResult;
      return true;
    } else {
      throw ParseException("sparqlPrefix");
    }
  } else {
    return false;
  }
}

// ________________________________________________________________
template <class T>
bool TurtleParser<T>::sparqlBase() {
  if (skip<TokId::SparqlBase>()) {
    if (iriref()) {
      _baseIRI = _lastParseResult;
      return true;
    } else {
      throw ParseException("sparqlBase");
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
      throw ParseException("triples");
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
    while (skip<TokId::Semicolon>()) {
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
    while (skip<TokId::Comma>() && check(object())) {
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
  if (auto [success, word] = _tok.template getNextToken<TokId::A>(); success) {
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
  if (!skip<TokId::OpenSquared>()) {
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
  check(skip<TokId::CloseSquared>());
  // restore subject and predicate
  _activeSubject = savedSubject;
  _activePredicate = savedPredicate;
  return true;
}

// _____________________________________________________________________
template <class T>
bool TurtleParser<T>::collection() {
  if (!skip<TokId::OpenRound>()) {
    return false;
  }
  throw ParseException(
      "We do not know how to handle collections in QLever yet\n");
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
  auto s = _lastParseResult;
  if (langtag()) {
    _lastParseResult = s + _lastParseResult;
    return true;
    // TODO<joka921> this allows spaces here since the ^^ is unique in the
    // sparql syntax. is this correct?
  } else if (skip<TokId::DoubleCircumflex>() && check(iri())) {
    _lastParseResult = s + "^^" + _lastParseResult;
    return true;
  } else {
    // it is okay to neither have a langtag nor a xsd datatype
    return true;
  }
}

// ______________________________________________________________________
template <class T>
bool TurtleParser<T>::booleanLiteral() {
  return parseTerminal<TokId::True>() || parseTerminal<TokId::False>();
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
    throw ParseException("unterminated string Literal");
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
  if (pnameLN() || pnameNS()) {
    _lastParseResult =
        '<' + expandPrefix(_activePrefix) + _lastParseResult + '>';
    return true;
  } else {
    return false;
  }
}

// _____________________________________________________________________
template <class T>
bool TurtleParser<T>::blankNode() {
  return blankNodeLabel() || anon();
}

// _______________________________________________________________________
template <class T>
template <TokId terminal>
bool TurtleParser<T>::parseTerminal() {
  _tok.skipWhitespaceAndComments();
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
  return parseTerminal<TokId::BlankNodeLabel>();
}

// __________________________________________________________________________
template <class T>
bool TurtleParser<T>::pnameNS() {
  if (parseTerminal<TokId::PnameNS>()) {
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
bool TurtleParser<T>::pnameLN() {
  // relaxed parsing, only works if the greedy parsing of the ":"
  // is ok
  _tok.skipWhitespaceAndComments();
  auto view = _tok.view();
  auto pos = view.find(":");
  if (pos == string::npos) {
    return false;
  }
  // these can also be part of a collection etc.
  // find any character that can end a pnameLN
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
  // manually check if the input starts with "<" and then find the next ">"
  // this might accept invalid irirefs but is faster than checking the
  // complete regexes
  _tok.skipWhitespaceAndComments();
  auto view = _tok.view();
  if (ad_utility::startsWith(view, "<")) {
    auto endPos = view.find_first_of("> \n");
    if (endPos == string::npos || view[endPos] != '>') {
      throw ParseException("Iriref");
    } else {
      _tok.remove_prefix(endPos + 1);
      _lastParseResult = view.substr(0, endPos + 1);
      return true;
    }
  } else {
    return false;
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
  buf.resize(_tok.data().size() + nextBytes.size());
  memcpy(buf.data(), _tok.data().begin(), _tok.data().size());
  memcpy(buf.data() + _tok.data().size(), nextBytes.data(), nextBytes.size());
  _byteVec = std::move(buf);
  _tok.reset(_byteVec.data(), _byteVec.size());

  LOG(TRACE) << "Succesfully decompressed next batch of " << nextBytes.size()
             << " << bytes to parser\n";

  // important: our tokenizer may have a new position
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
          LOG(INFO) << std::string_view(d.data(), s);
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
              throw typename TurtleParser<T>::ParseException(
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

template class TurtleParser<Tokenizer>;
template class TurtleParser<TokenizerCtre>;
template class TurtleStreamParser<Tokenizer>;
template class TurtleStreamParser<TokenizerCtre>;
template class TurtleMmapParser<Tokenizer>;
template class TurtleMmapParser<TokenizerCtre>;
