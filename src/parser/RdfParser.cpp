// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#include "parser/RdfParser.h"

#include <absl/strings/charconv.h>

#include <cstring>
#include <exception>
#include <optional>

#include "global/Constants.h"
#include "parser/GeoPoint.h"
#include "parser/NormalizedString.h"
#include "parser/RdfEscaping.h"
#include "util/Conversions.h"
#include "util/DateYearDuration.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"

using namespace std::chrono_literals;
// _______________________________________________________________
template <class T>
bool TurtleParser<T>::statement() {
  tok_.skipWhitespaceAndComments();
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
      prefixMap_[activePrefix_] = lastParseResult_.getIri();
      return true;
    } else {
      raise("Parsing @prefix definition failed");
    }
  } else {
    return false;
  }
}

// ________________________________________________________________
template <class T>
bool TurtleParser<T>::base() {
  if (skip<TurtleTokenId::TurtleBase>()) {
    if (iriref() && check(skip<TurtleTokenId::Dot>())) {
      prefixMap_[""] = lastParseResult_.getIri();
      return true;
    } else {
      raise("Parsing @base definition failed");
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
      prefixMap_[activePrefix_] = lastParseResult_.getIri();
      return true;
    } else {
      raise("Parsing PREFIX definition failed");
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
      prefixMap_[""] = lastParseResult_.getIri();
      return true;
    } else {
      raise("Parsing BASE definition failed");
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
      raise("Parsing predicate or object failed");
    }
  } else {
    if (blankNodePropertyList()) {
      activeSubject_ = lastParseResult_;
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
  tok_.skipWhitespaceAndComments();
  if (auto [success, word] = tok_.template getNextToken<TurtleTokenId::A>();
      success) {
    (void)word;
    activePredicate_ = TripleComponent::Iri::fromIriref(
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
template <class T>
bool TurtleParser<T>::subject() {
  if (blankNode() || iri() || collection()) {
    activeSubject_ = lastParseResult_;
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
template <class T>
bool TurtleParser<T>::predicate() {
  if (iri()) {
    activePredicate_ = lastParseResult_.getIri();
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
  // TODO<joka921> Currently collections and blankNodePropertyLists do not work
  // on dblp when using the relaxed parser. Is this fixable?
  if (blankNode() || literal() || iri() || collection() ||
      blankNodePropertyList()) {
    emitTriple();
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
  auto savedSubject = activeSubject_;
  auto savedPredicate = activePredicate_;
  // new triple with blank node as object
  string blank = createAnonNode();
  // the following triples have the blank node as subject
  activeSubject_ = blank;
  check(predicateObjectList());
  check(skip<TurtleTokenId::CloseSquared>());
  // restore subject and predicate
  activeSubject_ = savedSubject;
  activePredicate_ = savedPredicate;
  // The parse result is the current
  lastParseResult_ = blank;
  return true;
}

// _____________________________________________________________________
template <class T>
bool TurtleParser<T>::collection() {
  if (!skip<TurtleTokenId::OpenRound>()) {
    return false;
  }
  std::vector<TripleComponent> objects;
  while (object()) {
    objects.push_back(std::move(lastParseResult_));
  }
  // The `object` rule creates triples, but those are incomplete in this case,
  // so we remove them again.
  triples_.resize(triples_.size() - objects.size());
  // TODO<joka921> Move such functionality into a general util.
  auto makeIri = [](std::string_view suffix) {
    return TripleComponent::Iri::fromIriref(
        absl::StrCat("<", RDF_PREFIX, suffix, ">"));
  };
  static const auto nil = TripleComponent{makeIri("nil")};
  static const auto first = makeIri("first");
  static const auto rest = makeIri("rest");

  if (objects.empty()) {
    lastParseResult_ = nil;
  } else {
    // Create a new blank node for each collection element.
    std::vector<TripleComponent> blankNodes;
    blankNodes.reserve(objects.size());
    for (size_t i = 0; i < objects.size(); ++i) {
      blankNodes.push_back(createAnonNode());
    }

    // The first blank node (the list head) will be the actual result (subject
    // or object of the triple that contains the collection.
    lastParseResult_ = blankNodes.front();

    // Add the triples for the linked list structure.
    for (size_t i = 0; i < blankNodes.size(); ++i) {
      triples_.push_back({blankNodes[i], first, objects[i]});
      triples_.push_back({blankNodes[i], rest,
                          i + 1 < blankNodes.size() ? blankNodes[i + 1] : nil});
    }
  }
  check(skip<TurtleTokenId::CloseRound>());
  return true;
}

// ____________________________________________________________________________
template <class T>
void TurtleParser<T>::parseDoubleConstant(std::string_view input) {
  double result;
  // The functions used below cannot deal with leading redundant '+' signs.
  if (input.starts_with('+')) {
    input.remove_prefix(1);
  }
  auto [firstNonMatching, errorCode] =
      absl::from_chars(input.data(), input.data() + input.size(), result);
  if (firstNonMatching != input.end() || errorCode != std::errc{}) {
    auto errorMessage = absl::StrCat(
        "Value ", input, " could not be parsed as a floating point value");
    raiseOrIgnoreTriple(errorMessage);
  }
  lastParseResult_ = result;
}

// ____________________________________________________________________________
template <class T>
void TurtleParser<T>::parseIntegerConstant(std::string_view input) {
  if (integerOverflowBehavior() ==
      TurtleParserIntegerOverflowBehavior::AllToDouble) {
    return parseDoubleConstant(input);
  }
  int64_t result{0};
  // The functions used below cannot deal with leading redundant '+' signs.
  if (input.starts_with('+')) {
    input.remove_prefix(1);
  }
  // We cannot directly store this in `lastParseResult_` because this might
  // overwrite `input`.
  auto [firstNonMatching, errorCode] =
      std::from_chars(input.data(), input.data() + input.size(), result);
  if (errorCode == std::errc::result_out_of_range) {
    if (integerOverflowBehavior() ==
        TurtleParserIntegerOverflowBehavior::OverflowingToDouble) {
      return parseDoubleConstant(input);
    } else {
      auto errorMessage = absl::StrCat(
          "Value ", input,
          " cannot be represented as an integer value inside QLever, "
          "make it a xsd:decimal/xsd:double literal or specify "
          "\"parser-integer-overflow-behavior\"");
      raiseOrIgnoreTriple(errorMessage);
    }
  } else if (firstNonMatching != input.end()) {
    auto errorMessage = absl::StrCat(
        "Value ", input, " could not be parsed as an integer value");
    raiseOrIgnoreTriple(errorMessage);
  }
  lastParseResult_ = result;
}

// ______________________________________________________________________
template <class T>
bool TurtleParser<T>::numericLiteral() {
  return doubleParse() || decimal() || integer();
}

// ______________________________________________________________________
template <class T>
bool TurtleParser<T>::integer() {
  if (parseTerminal<TurtleTokenId::Integer>()) {
    parseIntegerConstant(lastParseResult_.getString());
    return true;
  } else {
    return false;
  }
}

// ______________________________________________________________________
template <class T>
bool TurtleParser<T>::decimal() {
  if (parseTerminal<TurtleTokenId::Decimal>()) {
    parseDoubleConstant(lastParseResult_.getString());
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________________
template <class T>
bool TurtleParser<T>::doubleParse() {
  if (parseTerminal<TurtleTokenId::Double>()) {
    parseDoubleConstant(lastParseResult_.getString());
    return true;
  } else {
    return false;
  }
}

// _____________________________________________________________________________
template <typename T>
bool NQuadParser<T>::statement() {
  if (!nQuadSubject()) {
    return false;
  }
  this->check(nQuadPredicate() && nQuadObject());
  if (!nQuadGraphLabel()) {
    activeGraphLabel_ = defaultGraphId_;
  }
  this->check(this->template skip<TurtleTokenId::Dot>());
  if (!this->currentTripleIgnoredBecauseOfInvalidLiteral_) {
    this->triples_.emplace_back(
        std::move(this->activeSubject_), std::move(this->activePredicate_),
        std::move(activeObject_), std::move(activeGraphLabel_));
  }
  this->currentTripleIgnoredBecauseOfInvalidLiteral_ = false;
  return true;
}

// _____________________________________________________________________________
template <typename T>
bool NQuadParser<T>::nQuadLiteral() {
  // Disallow multiline literals;
  return Base::rdfLiteralImpl(false);
}

// _____________________________________________________________________________
template <typename T>
bool NQuadParser<T>::nQuadSubject() {
  if (Base::iriref() || Base::blankNodeLabel()) {
    this->activeSubject_ = std::move(this->lastParseResult_);
    return true;
  }
  return false;
}

// _____________________________________________________________________________

template <typename T>
bool NQuadParser<T>::nQuadPredicate() {
  if (Base::iriref()) {
    this->activePredicate_ = std::move(this->lastParseResult_.getIri());
    return true;
  }
  return false;
}

template <typename T>
bool NQuadParser<T>::nQuadObject() {
  if (Base::iriref() || Base::blankNodeLabel() || nQuadLiteral()) {
    this->activeObject_ = std::move(this->lastParseResult_);
    return true;
  }
  return false;
}

// _____________________________________________________________________________
template <typename T>
bool NQuadParser<T>::nQuadGraphLabel() {
  if (Base::iriref() || Base::blankNodeLabel()) {
    activeGraphLabel_ = std::move(this->lastParseResult_);
    return true;
  }
  return false;
}

// ______________________________________________________________________
template <class T>
bool TurtleParser<T>::rdfLiteralImpl(bool allowMultilineLiterals) {
  if (!stringParseImpl(allowMultilineLiterals)) {
    return false;
  }

  auto previous = lastParseResult_.getLiteral();
  if (langtag()) {
    previous.addLanguageTag(lastParseResult_.getString());
    lastParseResult_ = std::move(previous);
  } else if (skip<TurtleTokenId::DoubleCircumflex>() && check(iri())) {
    literalAndDatatypeToTripleComponentImpl(
        asStringViewUnsafe(previous.getContent()), lastParseResult_.getIri(),
        this);
  }

  // It is okay to neither have a langtag nor an XSD datatype.
  return true;
}

// ______________________________________________________________________
template <class T>
TripleComponent TurtleParser<T>::literalAndDatatypeToTripleComponentImpl(
    std::string_view normalizedLiteralContent,
    const TripleComponent::Iri& typeIri, TurtleParser<T>* parser) {
  auto literal = TripleComponent::Literal::literalWithNormalizedContent(
      asNormalizedStringViewUnsafe(normalizedLiteralContent));
  std::string_view type = asStringViewUnsafe(typeIri.getContent());

  // Helper to handle literals that are invalid for the respective datatype
  auto makeNormalLiteral = [&parser, &literal, normalizedLiteralContent,
                            type](std::optional<std::exception> error =
                                      std::nullopt) {
    std::string_view errorMsg = error.has_value() ? error.value().what() : "";
    std::string_view sep = error.has_value() ? ": " : "";
    LOG(DEBUG) << normalizedLiteralContent
               << " could not be parsed as an object of type " << type << sep
               << errorMsg
               << ". It is treated as a plain string literal without datatype "
                  "instead."
               << std::endl;
    parser->lastParseResult_ = std::move(literal);
  };

  try {
    if (ad_utility::contains(integerDatatypes_, type)) {
      parser->parseIntegerConstant(normalizedLiteralContent);
    } else if (type == XSD_BOOLEAN_TYPE) {
      if (normalizedLiteralContent == "true") {
        parser->lastParseResult_ = true;
      } else if (normalizedLiteralContent == "false") {
        parser->lastParseResult_ = false;
      } else {
        makeNormalLiteral();
      }
    } else if (ad_utility::contains(floatDatatypes_, type)) {
      parser->parseDoubleConstant(normalizedLiteralContent);
    } else if (type == XSD_DATETIME_TYPE) {
      parser->lastParseResult_ =
          DateYearOrDuration::parseXsdDatetime(normalizedLiteralContent);
    } else if (type == XSD_DATE_TYPE) {
      parser->lastParseResult_ =
          DateYearOrDuration::parseXsdDate(normalizedLiteralContent);
    } else if (type == XSD_GYEARMONTH_TYPE) {
      parser->lastParseResult_ =
          DateYearOrDuration::parseGYearMonth(normalizedLiteralContent);
    } else if (type == XSD_GYEAR_TYPE) {
      parser->lastParseResult_ =
          DateYearOrDuration::parseGYear(normalizedLiteralContent);
    } else if (type == XSD_DAYTIME_DURATION_TYPE) {
      parser->lastParseResult_ =
          DateYearOrDuration::parseXsdDayTimeDuration(normalizedLiteralContent);
    } else if (type == GEO_WKT_LITERAL) {
      // Not all WKT literals represent points (we can only fold points)
      auto geopoint = GeoPoint::parseFromLiteral(literal, false);
      if (geopoint.has_value()) {
        parser->lastParseResult_ = geopoint.value();
      } else {
        literal.addDatatype(typeIri);
        parser->lastParseResult_ = std::move(literal);
      }
    } else {
      literal.addDatatype(typeIri);
      parser->lastParseResult_ = std::move(literal);
    }
  } catch (const DateParseException& ex) {
    makeNormalLiteral(ex);
  } catch (const DateOutOfRangeException& ex) {
    makeNormalLiteral(ex);
  } catch (const DurationParseException& ex) {
    makeNormalLiteral(ex);
  } catch (const DurationOverflowException& ex) {
    makeNormalLiteral(ex);
  } catch (const CoordinateOutOfRangeException& ex) {
    makeNormalLiteral(ex);
  } catch (const std::exception& e) {
    parser->raise(e.what());
  }
  return parser->lastParseResult_;
}

// ______________________________________________________________________
template <class T>
TripleComponent TurtleParser<T>::literalAndDatatypeToTripleComponent(
    std::string_view normalizedLiteralContent,
    const TripleComponent::Iri& typeIri) {
  RdfStringParser<TurtleParser<T>> parser;

  return literalAndDatatypeToTripleComponentImpl(normalizedLiteralContent,
                                                 typeIri, &parser);
}

// ______________________________________________________________________
template <class T>
bool TurtleParser<T>::booleanLiteral() {
  if (parseTerminal<TurtleTokenId::True>()) {
    lastParseResult_ = true;
    return true;
  } else if (parseTerminal<TurtleTokenId::False>()) {
    lastParseResult_ = false;
    return true;
  } else {
    return false;
  }
}

// ______________________________________________________________________
template <class T>
bool TurtleParser<T>::stringParseImpl(bool allowMultilineLiterals) {
  // manually parse strings for efficiency
  auto view = tok_.view();
  size_t startPos = 0;
  size_t endPos = 1;
  std::array<string, 4> quotes{R"(""")", R"(''')", "\"", "\'"};
  bool foundString = false;
  for (const auto& q : quotes) {
    if (view.starts_with(q)) {
      foundString = true;
      startPos = q.size();
      if (!allowMultilineLiterals && q.size() > 1) {
        return false;
      }
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
    raise("Unterminated string literal");
  }
  // also include the quotation marks in the word
  lastParseResult_ = TripleComponent::Literal::fromEscapedRdfLiteral(
      view.substr(0, endPos + startPos));
  tok_.remove_prefix(endPos + startPos);
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
  lastParseResult_ = TripleComponent::Iri::fromPrefixAndSuffix(
      expandPrefix(activePrefix_), lastParseResult_.getString());
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
    tok_.skipWhitespaceAndComments();
  }
  auto [success, word] = tok_.template getNextToken<terminal>();
  if (success) {
    lastParseResult_ = word;
    return true;
  } else {
    return false;
  }
  return false;
}

// ________________________________________________________________________
template <class T>
bool TurtleParser<T>::blankNodeLabel() {
  bool res = parseTerminal<TurtleTokenId::BlankNodeLabel>();
  if (res) {
    // Add a special prefix to ensure that the manually specified blank nodes
    // never interfere with the automatically generated ones. The `substr`
    // removes the leading `_:` which will be added again by the `BlankNode`
    // constructor.
    lastParseResult_ =
        BlankNode{false, lastParseResult_.getString().substr(2)}.toSparql();
  }
  return res;
}

// __________________________________________________________________________
template <class T>
bool TurtleParser<T>::pnameNS() {
  if (parseTerminal<TurtleTokenId::PnameNS>()) {
    // this also includes a ":" which we do not need, hence the "-1"
    activePrefix_ = lastParseResult_.getString().substr(
        0, lastParseResult_.getString().size() - 1);
    lastParseResult_ = "";
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
  tok_.skipWhitespaceAndComments();
  auto view = tok_.view();
  auto pos = view.find(':');
  if (pos == string::npos) {
    return false;
  }
  // these can also be part of a collection etc.
  // find any character that can end a pnameLn when assuming that no
  // escape sequences were used
  auto posEnd = view.find_first_of(" \t\r\n,;", pos);
  if (posEnd == string::npos) {
    // make tests work
    posEnd = view.size();
  }
  // TODO<joka921>: Is it allowed to have no space between triples and the
  // dots? In this case we have to check something here.
  activePrefix_ = view.substr(0, pos);
  lastParseResult_ = view.substr(pos + 1, posEnd - (pos + 1));
  // we do not remove the whitespace or the ,; since they are needed
  tok_.remove_prefix(posEnd);
  return true;
}

// _____________________________________________________________________
template <class T>
bool TurtleParser<T>::iriref() {
  // First make a cheap and simple check if we find `<...>` in the current
  // line (we don't care about the characters in between). If not, this is
  // certainly not an IRI reference.
  tok_.skipWhitespaceAndComments();
  auto view = tok_.view();
  if (!view.starts_with('<')) {
    return false;
  }
  auto endPos = view.find_first_of("<>\"\n", 1);
  if (endPos == string::npos || view[endPos] != '>') {
    raise(
        "Unterminated IRI reference (found '<' but no '>' before "
        "one of the following characters: <, \", newline)");
  }
  // In relaxed mode, that is all we check. Otherwise, we check if the IRI is
  // standard-compliant. If not, we output a warning and try to parse it in a
  // more relaxed way.
  if constexpr (UseRelaxedParsing) {
    tok_.remove_prefix(endPos + 1);
    lastParseResult_ =
        TripleComponent::Iri::fromIriref(view.substr(0, endPos + 1));
    return true;
  } else {
    if (!parseTerminal<TurtleTokenId::Iriref>()) {
      LOG(WARN) << "IRI ref not standard-compliant: "
                << view.substr(0, endPos + 1) << std::endl;
      if (!parseTerminal<TurtleTokenId::IrirefRelaxed>()) {
        return false;
      }
    }
    lastParseResult_ =
        TripleComponent::Iri::fromIriref(lastParseResult_.getString());
    return true;
  }
}

// ______________________________________________________________________
template <class T>
typename RdfStreamParser<T>::TurtleParserBackupState
RdfStreamParser<T>::backupState() const {
  TurtleParserBackupState b;
  b.numBlankNodes_ = this->numBlankNodes_;
  b.numTriples_ = this->triples_.size();
  b.tokenizerPosition_ = this->tok_.data().begin();
  b.tokenizerSize_ = this->tok_.data().size();
  return b;
}

// _______________________________________________________________
template <class T>
bool RdfStreamParser<T>::resetStateAndRead(
    RdfStreamParser::TurtleParserBackupState* bPtr) {
  auto& b = *bPtr;
  auto nextBytesOpt = fileBuffer_->getNextBlock();
  if (!nextBytesOpt || nextBytesOpt.value().empty()) {
    // there are no more decompressed bytes, just continue with what we've got
    // do not alter any internal state.
    return false;
  }

  auto nextBytes = std::move(nextBytesOpt.value());

  // return to the state of the last backup
  this->numBlankNodes_ = b.numBlankNodes_;
  AD_CONTRACT_CHECK(this->triples_.size() >= b.numTriples_);
  this->triples_.resize(b.numTriples_);
  this->tok_.reset(b.tokenizerPosition_, b.tokenizerSize_);

  ParallelBuffer::BufferType buf;

  // Used for a more informative error message when a parse error occurs (see
  // function "raise").
  numBytesBeforeCurrentBatch_ += byteVec_.size() - tok_.data().size();
  buf.resize(tok_.data().size() + nextBytes.size());
  memcpy(buf.data(), tok_.data().begin(), tok_.data().size());
  memcpy(buf.data() + tok_.data().size(), nextBytes.data(), nextBytes.size());
  byteVec_ = std::move(buf);
  tok_.reset(byteVec_.data(), byteVec_.size());

  LOG(TRACE) << "Successfully decompressed next batch of " << nextBytes.size()
             << " << bytes to parser\n";

  // repair the backup state, its pointers might have changed due to
  // reallocation
  b = backupState();
  return true;
}

template <class T>
void RdfStreamParser<T>::initialize(const string& filename) {
  this->clear();
  fileBuffer_ = std::make_unique<ParallelFileBuffer>(bufferSize_);
  fileBuffer_->open(filename);
  byteVec_.resize(bufferSize_);
  // decompress the first block and initialize Tokenizer
  if (auto res = fileBuffer_->getNextBlock(); res) {
    byteVec_ = std::move(res.value());
    tok_.reset(byteVec_.data(), byteVec_.size());
  } else {
    LOG(WARN)
        << "The input stream for the turtle parser seems to contain no data!\n";
  }
}

template <class T>
bool RdfStreamParser<T>::getLine(TurtleTriple* triple) {
  if (triples_.empty()) {
    // if parsing the line fails because our buffer ends before the end of
    // the next statement we need to be able to recover
    TurtleParserBackupState b = backupState();
    // always try to parse a batch of triples at once to make up for the
    // relatively expensive backup calls.
    while (triples_.size() < PARSER_MIN_TRIPLES_AT_ONCE &&
           !isParserExhausted_) {
      bool parsedStatement;
      std::optional<ParseException> ex;
      // If this buffer reads from a memory-mapped file, then exceptions are
      // immediately rethrown. If we are reading from a stream in chunks of
      // bytes, we can try again with a larger buffer.
      try {
        // variable parsedStatement will be true iff a statement can
        // successfully be parsed
        parsedStatement = T::statement();
      } catch (const typename T::ParseException& p) {
        parsedStatement = false;
        ex = p;
      }

      if (!parsedStatement) {
        // we read chunks of memories in a buffered way
        // try to parse with a larger buffer and repeat the reading process
        // (maybe the failure was due to statements crossing our block).
        if (resetStateAndRead(&b)) {
          // we have successfully extended our buffer
          if (byteVec_.size() > BZIP2_MAX_TOTAL_BUFFER_SIZE) {
            auto d = tok_.view();
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
            LOG(INFO) << std::string_view(d.data(), s) << std::endl;
            if (ex.has_value()) {
              throw ex.value();

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
          if (ex.has_value()) {
            throw ex.value();
          } else {
            // we are at the end of an input stream without an exception
            // the input is exhausted, but we still may retrieve
            // triples parsed so far, check if we have indeed parsed through
            // the complete input
            tok_.skipWhitespaceAndComments();
            auto d = tok_.view();
            if (!d.empty()) {
              LOG(INFO) << "Parsing of line has Failed, but parseInput is not "
                           "yet exhausted. Remaining bytes: "
                        << d.size() << '\n';
              auto s = std::min(size_t(1000), size_t(d.size()));
              LOG(INFO) << "Logging first 1000 unparsed characters\n";
              LOG(INFO) << std::string_view(d.data(), s) << std::endl;
            }
            isParserExhausted_ = true;
            break;
          }
        }
      }
    }
  }

  // if we have a triple now we can return it, else we are done parsing.
  if (triples_.empty()) {
    return false;
  }

  // we now have at least one triple, return it.
  *triple = triples_.back();
  triples_.pop_back();
  return true;
}

// We will use the  following trick: For a batch that is forwarded to the
// parallel parser, we will first increment `numBatchesTotal_` and then call
// the following lambda after the batch has completely been parsed and the
// result pushed to the `tripleCollector_`. We thus get the invariant that
// `batchIdx_
// == numBatchesTotal_` iff all batches that have been inserted to the
// `parallelParser_` have been fully processed. After the last batch we will
// push another call to this lambda to the `parallelParser_` which will then
// finish the `tripleCollector_` as soon as all batches have been computed.
template <typename Tokenizer_T>
void RdfParallelParser<Tokenizer_T>::finishTripleCollectorIfLastBatch() {
  if (batchIdx_.fetch_add(1) == numBatchesTotal_) {
    tripleCollector_.finish();
  }
}

// __________________________________________________________________________________
template <typename Tokenizer_T>
void RdfParallelParser<Tokenizer_T>::parseBatch(size_t parsePosition,
                                                auto batch) {
  try {
    RdfStringParser<Tokenizer_T> parser;
    parser.prefixMap_ = this->prefixMap_;
    parser.setPositionOffset(parsePosition);
    parser.setInputStream(std::move(batch));
    // TODO: raise error message if a prefix parsing fails;
    std::vector<TurtleTriple> triples = parser.parseAndReturnAllTriples();

    tripleCollector_.push([triples = std::move(triples), this]() mutable {
      triples_ = std::move(triples);
    });
    finishTripleCollectorIfLastBatch();
  } catch (std::exception& e) {
    tripleCollector_.pushException(std::current_exception());
    parallelParser_.finish();
  }
};

// _______________________________________________________________________
template <typename Tokenizer_T>
void RdfParallelParser<Tokenizer_T>::feedBatchesToParser(
    auto remainingBatchFromInitialization) {
  bool first = true;
  size_t parsePosition = 0;
  auto cleanup =
      ad_utility::makeOnDestructionDontThrowDuringStackUnwinding([this] {
        // Wait until everything has been parsed and then also finish the
        // triple collector.
        parallelParser_.push([this] { finishTripleCollectorIfLastBatch(); });
        parallelParser_.finish();
      });
  decltype(remainingBatchFromInitialization) inputBatch;
  try {
    while (true) {
      if (first) {
        inputBatch = std::move(remainingBatchFromInitialization);
        first = false;
      } else {
        auto nextOptional = fileBuffer_.getNextBlock();
        if (!nextOptional) {
          return;
        }
        inputBatch = std::move(nextOptional.value());
      }
      auto batchSize = inputBatch.size();
      auto parseThisBatch = [this, parsePosition,
                             batch = std::move(inputBatch)]() mutable {
        return parseBatch(parsePosition, std::move(batch));
      };
      parsePosition += batchSize;
      numBatchesTotal_.fetch_add(1);
      if (sleepTimeForTesting_ > 0ms) {
        std::this_thread::sleep_for(sleepTimeForTesting_);
      }
      bool stillActive = parallelParser_.push(parseThisBatch);
      if (!stillActive) {
        return;
      }
    }
  } catch (std::exception& e) {
    tripleCollector_.pushException(std::current_exception());
  }
};

// _______________________________________________________________________
template <typename Tokenizer_T>
void RdfParallelParser<Tokenizer_T>::initialize(const string& filename) {
  ParallelBuffer::BufferType remainingBatchFromInitialization;
  fileBuffer_.open(filename);
  if (auto batch = fileBuffer_.getNextBlock(); !batch) {
    LOG(WARN) << "Empty input to the TURTLE parser, is this what you intended?"
              << std::endl;
  } else {
    RdfStringParser<Tokenizer_T> declarationParser{};
    declarationParser.setInputStream(std::move(batch.value()));
    while (declarationParser.parseDirectiveManually()) {
    }
    this->prefixMap_ = std::move(declarationParser.getPrefixMap());
    auto remainder = declarationParser.getUnparsedRemainder();
    remainingBatchFromInitialization.reserve(remainder.size());
    std::ranges::copy(remainder,
                      std::back_inserter(remainingBatchFromInitialization));
  }

  auto feedBatches = [this, firstBatch = std::move(
                                remainingBatchFromInitialization)]() mutable {
    return feedBatchesToParser(std::move(firstBatch));
  };

  parseFuture_ = std::async(std::launch::async, feedBatches);
}

// _______________________________________________________________________
template <class T>
bool RdfParallelParser<T>::getLine(TurtleTriple* triple) {
  // If the current batch is out of triples_ get the next batch of triples.
  // We need a while loop instead of a simple if in case there is a batch that
  // contains no triples. (Theoretically this might happen, and it is safer this
  // way)
  while (triples_.empty()) {
    auto optionalTripleTask = tripleCollector_.pop();
    if (!optionalTripleTask) {
      // Everything has been parsed
      return false;
    }
    // OptionalTripleTask fills the triples_ vector
    (*optionalTripleTask)();
  }

  // we now have at least one triple, return it.
  *triple = std::move(triples_.back());
  triples_.pop_back();
  return true;
}

// _______________________________________________________________________
template <class T>
std::optional<std::vector<TurtleTriple>> RdfParallelParser<T>::getBatch() {
  // we need a while in case there is a batch that contains no triples
  // (this should be rare, // TODO warn about this
  while (triples_.empty()) {
    auto optionalTripleTask = tripleCollector_.pop();
    if (!optionalTripleTask) {
      // everything has been parsed
      return std::nullopt;
    }
    (*optionalTripleTask)();
  }

  return std::move(triples_);
}

// __________________________________________________________
template <typename T>
RdfParallelParser<T>::~RdfParallelParser() {
  ad_utility::ignoreExceptionIfThrows(
      [this] {
        parallelParser_.finish();
        tripleCollector_.finish();
        parseFuture_.get();
      },
      "During the destruction of a RdfParallelParser");
}

// Explicit instantiations
template class TurtleParser<Tokenizer>;
template class TurtleParser<TokenizerCtre>;
template class RdfStreamParser<TurtleParser<Tokenizer>>;
template class RdfStreamParser<TurtleParser<TokenizerCtre>>;
template class RdfParallelParser<TurtleParser<Tokenizer>>;
template class RdfParallelParser<TurtleParser<TokenizerCtre>>;
template class RdfStreamParser<NQuadParser<Tokenizer>>;
template class RdfStreamParser<NQuadParser<TokenizerCtre>>;
template class RdfParallelParser<NQuadParser<Tokenizer>>;
template class RdfParallelParser<NQuadParser<TokenizerCtre>>;
