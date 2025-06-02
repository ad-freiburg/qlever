// Copyright 2018 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "parser/RdfParser.h"

#include <absl/functional/bind_front.h>
#include <absl/strings/charconv.h>

#include <cstring>
#include <exception>
#include <optional>

#include "engine/CallFixedSize.h"
#include "global/Constants.h"
#include "parser/GeoPoint.h"
#include "parser/NormalizedString.h"
#include "parser/Tokenizer.h"
#include "parser/TokenizerCtre.h"
#include "util/DateYearDuration.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/TransparentFunctors.h"

using namespace std::chrono_literals;

// _____________________________________________________________________________
template <class Tokenizer_T>
void TurtleParser<Tokenizer_T>::clear() {
  lastParseResult_ = "";

  activeSubject_ = TripleComponent::Iri::fromIriref("<>");
  activePredicate_ = TripleComponent::Iri::fromIriref("<>");
  activePrefix_.clear();

  prefixMap_ = prefixMapDefault_;

  tok_.reset(nullptr, 0);
  triples_.clear();
  numBlankNodes_ = 0;
  isParserExhausted_ = false;
}

// _____________________________________________________________________________
template <class T>
bool TurtleParser<T>::statement() {
  tok_.skipWhitespaceAndComments();
  return directive() || (triples() && skip<TurtleTokenId::Dot>());
}

// _____________________________________________________________________________
template <class Tokenizer_T>
void TurtleParser<Tokenizer_T>::raise(std::string_view error_message) const {
  auto d = tok_.view();
  std::stringstream errorMessage;
  errorMessage << "Parse error at byte position " << getParsePosition() << ": "
               << error_message << '\n';
  if (!d.empty()) {
    size_t num_bytes = 500;
    auto s = std::min(size_t(num_bytes), size_t(d.size()));
    errorMessage << "The next " << num_bytes << " bytes are:\n"
                 << std::string_view(d.data(), s) << '\n';
  }
  throw ParseException{std::move(errorMessage).str()};
}

// _____________________________________________________________________________
template <class Tokenizer_T>
void TurtleParser<Tokenizer_T>::raiseOrIgnoreTriple(
    std::string_view errorMessage) {
  if (invalidLiteralsAreSkipped()) {
    currentTripleIgnoredBecauseOfInvalidLiteral_ = true;
  } else {
    raise(errorMessage);
  }
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
      setPrefixOrThrow(activePrefix_, lastParseResult_.getIri());
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
      const auto& iri = lastParseResult_.getIri();
      setPrefixOrThrow(baseForRelativeIriKey_, iri.getBaseIri(false));
      setPrefixOrThrow(baseForAbsoluteIriKey_, iri.getBaseIri(true));
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
      setPrefixOrThrow(activePrefix_, lastParseResult_.getIri());
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
      const auto& iri = lastParseResult_.getIri();
      setPrefixOrThrow(baseForRelativeIriKey_, iri.getBaseIri(false));
      setPrefixOrThrow(baseForAbsoluteIriKey_, iri.getBaseIri(true));
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
  return predicate() || predicateSpecialA();
}

// ___________________________________________________________________
template <class T>
bool TurtleParser<T>::predicateSpecialA() {
  if (parseTerminal<TurtleTokenId::A>()) {
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

// _____________________________________________________________________________
template <class Tokenizer_T>
void TurtleParser<Tokenizer_T>::raiseDisallowedPrefixOrBaseError() const {
  AD_CORRECTNESS_CHECK(prefixAndBaseDisabled_);
  raise(
      "@prefix or @base directives need to be at the beginning of the file "
      "when using the parallel parser. Later redundant redefinitions are "
      "fine. Use '--parse-parallel false' if you can't guarantee this. If "
      "the reason for this error is that the input is a concatenation of "
      "Turtle files, each of which has the prefixes at the beginning, you "
      "should feed the files to QLever separately instead of concatenated");
}

// _____________________________________________________________________________
template <class Tokenizer_T>
void TurtleParser<Tokenizer_T>::setPrefixOrThrow(
    const std::string& key, const ad_utility::triple_component::Iri& prefix) {
  if (prefixAndBaseDisabled_ &&
      (!prefixMap_.contains(key) || prefixMap_[key] != prefix)) {
    raiseDisallowedPrefixOrBaseError();
  }
  prefixMap_[key] = prefix;
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
  if constexpr (T::UseRelaxedParsing) {
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

// _____________________________________________________________________________
template <class Tokenizer_T>
void TurtleParser<Tokenizer_T>::emitTriple() {
  if (!currentTripleIgnoredBecauseOfInvalidLiteral_) {
    triples_.emplace_back(activeSubject_, activePredicate_, lastParseResult_,
                          defaultGraphIri_);
  }
  currentTripleIgnoredBecauseOfInvalidLiteral_ = false;
}

// _____________________________________________________________________________
template <class Tokenizer_T>
bool TurtleParser<Tokenizer_T>::check(bool result) const {
  if (result) {
    return true;
  } else {
    raise("A check for a required element failed");
  }
}

// _____________________________________________________________________________
template <class Tokenizer_T>
TripleComponent::Iri TurtleParser<Tokenizer_T>::expandPrefix(
    const std::string& prefix) {
  if (!prefixMap_.count(prefix)) {
    raise("Prefix " + prefix +
          " was not previously defined using a PREFIX or @prefix "
          "declaration");
  } else {
    return prefixMap_[prefix];
  }
}

// _____________________________________________________________________________
template <class Tokenizer_T>
std::string TurtleParser<Tokenizer_T>::createAnonNode() {
  return BlankNode{true, absl::StrCat(blankNodePrefix_, "_", numBlankNodes_++)}
      .toSparql();
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

// _____________________________________________________________________________
template <class Tokenizer_T>
bool TurtleParser<Tokenizer_T>::anon() {
  if (!parseTerminal<TurtleTokenId::Anon>()) {
    return false;
  }
  lastParseResult_ = createAnonNode();
  return true;
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
  if constexpr (T::UseRelaxedParsing) {
    tok_.remove_prefix(endPos + 1);
    lastParseResult_ = TripleComponent::Iri::fromIrirefConsiderBase(
        view.substr(0, endPos + 1), baseForRelativeIri(), baseForAbsoluteIri());
    return true;
  } else {
    if (!parseTerminal<TurtleTokenId::Iriref>()) {
      LOG(WARN) << "IRI ref not standard-compliant: "
                << view.substr(0, endPos + 1) << std::endl;
      if (!parseTerminal<TurtleTokenId::IrirefRelaxed>()) {
        return false;
      }
    }
    lastParseResult_ = TripleComponent::Iri::fromIrirefConsiderBase(
        lastParseResult_.getString(), baseForRelativeIri(),
        baseForAbsoluteIri());
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
  AD_CORRECTNESS_CHECK(fileBuffer_);
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
void RdfStreamParser<T>::initialize(const string& filename,
                                    ad_utility::MemorySize bufferSize) {
  this->clear();
  // Make sure that a block of data ends with a newline. This is important for
  // two reasons:
  //
  // 1. A block of data must not end in the middle of a comment. Otherwise the
  // remaining part of the comment, which is prepended to the next block, is
  // not recognized as a comment.
  //
  // 2. A block of data must not end with a `.` (without subsequent newline).
  // The reason is that with a `.` at the end, we cannot decide whether we are
  // in the middle of a `PN_LOCAL` (that continues in the next buffer) or at the
  // end of a statement.
  fileBuffer_ = std::make_unique<ParallelBufferWithEndRegex>(
      bufferSize.getBytes(), "([\\r\\n]+)");
  fileBuffer_->open(filename);
  byteVec_.resize(bufferSize.getBytes());
  // decompress the first block and initialize Tokenizer
  if (auto res = fileBuffer_->getNextBlock(); res) {
    byteVec_ = std::move(res.value());
    tok_.reset(byteVec_.data(), byteVec_.size());
  } else {
    LOG(WARN)
        << "The input stream for the turtle parser seems to contain no data!\n";
  }
}

// _____________________________________________________________________________
template <class T>
bool RdfStreamParser<T>::getLineImpl(TurtleTriple* triple) {
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
template <typename T>
void RdfParallelParser<T>::finishTripleCollectorIfLastBatch() {
  if (batchIdx_.fetch_add(1) == numBatchesTotal_) {
    tripleCollector_.finish();
  }
}

// __________________________________________________________________________________
template <typename T>
template <typename Batch>
void RdfParallelParser<T>::parseBatch(size_t parsePosition, Batch batch) {
  try {
    RdfStringParser<T> parser{defaultGraphIri_};
    parser.prefixMap_ = this->prefixMap_;
    parser.disablePrefixParsing();
    parser.setPositionOffset(parsePosition);
    parser.setInputStream(std::move(batch));
    // TODO: raise error message if a prefix parsing fails;
    std::vector<TurtleTriple> triples = parser.parseAndReturnAllTriples();

    tripleCollector_.push([triples = std::move(triples), this]() mutable {
      triples_ = std::move(triples);
    });
    finishTripleCollectorIfLastBatch();
  } catch (std::exception& e) {
    errorMessages_.wlock()->emplace_back(parsePosition, e.what());
    tripleCollector_.pushException(std::current_exception());
    parallelParser_.finish();
  }
};

// _______________________________________________________________________
template <typename T>
template <typename Batch>
void RdfParallelParser<T>::feedBatchesToParser(
    Batch remainingBatchFromInitialization) {
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
        auto nextOptional = fileBuffer_->getNextBlock();
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
    errorMessages_.wlock()->emplace_back(parsePosition, e.what());
    tripleCollector_.pushException(std::current_exception());
  }
};

// _______________________________________________________________________
template <typename T>
void RdfParallelParser<T>::initialize(const string& filename,
                                      ad_utility::MemorySize bufferSize) {
  fileBuffer_ = std::make_unique<ParallelBufferWithEndRegex>(
      bufferSize.getBytes(), "\\.[\\t ]*([\\r\\n]+)");
  ParallelBuffer::BufferType remainingBatchFromInitialization;
  fileBuffer_->open(filename);
  if (auto batch = fileBuffer_->getNextBlock(); !batch) {
    LOG(WARN) << "Empty input to the TURTLE parser, is this what you intended?"
              << std::endl;
  } else {
    RdfStringParser<T> declarationParser{};
    declarationParser.setInputStream(std::move(batch.value()));
    while (declarationParser.parseDirectiveManually()) {
    }
    this->prefixMap_ = std::move(declarationParser.getPrefixMap());
    auto remainder = declarationParser.getUnparsedRemainder();
    remainingBatchFromInitialization.reserve(remainder.size());
    ql::ranges::copy(remainder,
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
bool RdfParallelParser<T>::getLineImpl(TurtleTriple* triple) {
  // If the current batch is out of triples_ get the next batch of triples.
  // We need a while loop instead of a simple if in case there is a batch that
  // contains no triples. (Theoretically this might happen, and it is safer this
  // way)
  while (triples_.empty()) {
    auto optionalTripleTask = [&]() {
      try {
        return tripleCollector_.pop();
      } catch (const std::exception&) {
        AD_LOG_ERROR << "Error detected during parallel parsing, waiting for "
                        "workers to finish ..."
                     << std::endl;
        // In case of multiple errors in parallel batches, we always report the
        // first error.
        parallelParser_.waitUntilFinished();
        auto errors = std::move(*errorMessages_.wlock());
        const auto& firstError =
            ql::ranges::min_element(errors, {}, ad_utility::first);
        AD_CORRECTNESS_CHECK(firstError != errors.end());
        throw std::runtime_error{firstError->second};
      }
    }();
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

// Create a parser for a single file of an `InputFileSpecification`. The type
// of the parser depends on the filetype (Turtle or N-Quads) and on whether the
// file is to be parsed in parallel.
template <typename TokenizerT>
static std::unique_ptr<RdfParserBase> makeSingleRdfParser(
    const Index::InputFileSpecification& file,
    ad_utility::MemorySize bufferSize) {
  auto graph = [file]() -> TripleComponent {
    if (file.defaultGraph_.has_value()) {
      return TripleComponent::Iri::fromIrirefWithoutBrackets(
          file.defaultGraph_.value());
    } else {
      return qlever::specialIds().at(DEFAULT_GRAPH_IRI);
    }
  };
  auto makeRdfParserImpl = ad_utility::ApplyAsValueIdentity{
      [&filename = file.filename_, &bufferSize, &graph](
          auto useParallel,
          auto isTurtleInput) -> std::unique_ptr<RdfParserBase> {
        using InnerParser =
            std::conditional_t<isTurtleInput == 1, TurtleParser<TokenizerT>,
                               NQuadParser<TokenizerT>>;
        using Parser =
            std::conditional_t<useParallel == 1, RdfParallelParser<InnerParser>,
                               RdfStreamParser<InnerParser>>;
        return std::make_unique<Parser>(filename, bufferSize, graph());
      }};

  // The call to `callFixedSize` lifts runtime integers to compile time
  // integers. We use it here to create the correct combination of template
  // arguments.
  return ad_utility::callFixedSize(
      std::array{file.parseInParallel_ ? 1 : 0,
                 file.filetype_ == Index::Filetype::Turtle ? 1 : 0},
      makeRdfParserImpl);
}

// _____________________________________________________________________________
std::optional<std::vector<TurtleTriple>> RdfParserBase::getBatch() {
  std::vector<TurtleTriple> result;
  result.reserve(100'000);
  for (size_t i = 0; i < 100'000; ++i) {
    result.emplace_back();
    bool success = getLine(result.back());
    if (!success) {
      result.resize(result.size() - 1);
      break;
    }
  }
  if (result.empty()) {
    return std::nullopt;
  }
  return result;
}

// ______________________________________________________________
RdfMultifileParser::RdfMultifileParser(
    const std::vector<qlever::InputFileSpecification>& files,
    ad_utility::MemorySize bufferSize) {
  using namespace qlever;
  // This lambda parses a single file and pushes the results and all occurring
  // exceptions to the `finishedBatchQueue_`.
  auto parseFile = [this](const InputFileSpecification& file,
                          ad_utility::MemorySize bufferSize) {
    try {
      auto parser = makeSingleRdfParser<Tokenizer>(file, bufferSize);
      while (auto batch = parser->getBatch()) {
        bool active = finishedBatchQueue_.push(std::move(batch.value()));
        if (!active) {
          // The queue was finished prematurely, stop this thread. This is
          // important to avoid deadlocks.
          return;
        }
      }
    } catch (...) {
      finishedBatchQueue_.pushException(std::current_exception());
      return;
    }
    if (numActiveParsers_.fetch_sub(1) == 1) {
      // We are the last parser, we have to notify the downstream code that the
      // input has been parsed completely.
      finishedBatchQueue_.finish();
    }
  };

  // Feed all the input files to the `parsingQueue_`.
  auto makeParsers = [files, bufferSize, this, parseFile]() {
    for (const auto& file : files) {
      numActiveParsers_++;
      bool active =
          parsingQueue_.push(absl::bind_front(parseFile, file, bufferSize));
      if (!active) {
        // The queue was finished prematurely, stop this thread. This is
        // important to avoid deadlocks.
        return;
      }
    }
    parsingQueue_.finish();
  };
  feederThread_ = ad_utility::JThread{makeParsers};
}

// _____________________________________________________________________________
RdfMultifileParser::~RdfMultifileParser() {
  ad_utility::ignoreExceptionIfThrows(
      [this] {
        parsingQueue_.finish();
        finishedBatchQueue_.finish();
      },
      "During the destruction of an RdfMultifileParser");
}

//______________________________________________________________________________
bool RdfMultifileParser::getLineImpl(TurtleTriple*) { AD_FAIL(); }

// _____________________________________________________________________________
std::optional<std::vector<TurtleTriple>> RdfMultifileParser::getBatch() {
  return finishedBatchQueue_.pop();
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
