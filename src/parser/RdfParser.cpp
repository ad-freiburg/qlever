// Copyright 2018 - 2026 The QLever Authors, in particular:
//
// 2024 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2018 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/RdfParser.h"

#include <absl/strings/charconv.h>

#include <cstring>
#include <exception>
#include <future>
#include <optional>

#include "backports/StartsWithAndEndsWith.h"
#include "global/Constants.h"
#include "index/EncodedIriManager.h"
#include "parser/AsyncMultifileParser.h"
#include "parser/AsyncSingleFileParser.h"
#include "parser/NormalizedString.h"
#include "parser/Tokenizer.h"
#include "parser/TokenizerCtre.h"
#include "rdfTypes/GeoPoint.h"
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

  prefixMap_ = {};
  baseIri_.reset();

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
      setBaseIriOrThrow(lastParseResult_.getIri());
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
      setBaseIriOrThrow(lastParseResult_.getIri());
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
    activePredicate_ = lastParseResult_;
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
  std::string blank = createAnonNode();
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
  if (ql::starts_with(input, '+')) {
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
  if (ql::starts_with(input, '+')) {
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
    this->triples_.push_back(
        {std::move(this->activeSubject_), std::move(this->activePredicate_),
         std::move(activeObject_), std::move(activeGraphLabel_)});
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
    this->activePredicate_ = std::move(this->lastParseResult_);
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
        asStringViewUnsafe(previous.getContent()), lastParseResult_.getIri());
  }

  // It is okay to neither have a langtag nor an XSD datatype.
  return true;
}

// ______________________________________________________________________
template <class T>
TripleComponent TurtleParser<T>::literalAndDatatypeToTripleComponentImpl(
    std::string_view normalizedLiteralContent,
    const TripleComponent::Iri& typeIri) {
  auto literal = TripleComponent::Literal::literalWithNormalizedContent(
      asNormalizedStringViewUnsafe(normalizedLiteralContent));
  std::string_view type = asStringViewUnsafe(typeIri.getContent());

  // Helper to handle literals that are invalid for the respective datatype
  auto makeNormalLiteral = [this, &literal, normalizedLiteralContent,
                            type](std::optional<std::exception> error =
                                      std::nullopt) {
    std::string_view errorMsg = error.has_value() ? error.value().what() : "";
    std::string_view sep = error.has_value() ? ": " : "";
    AD_LOG_DEBUG
        << normalizedLiteralContent
        << " could not be parsed as an object of type " << type << sep
        << errorMsg
        << ". It is treated as a plain string literal without datatype "
           "instead."
        << std::endl;
    lastParseResult_ = std::move(literal);
  };

  try {
    if (ad_utility::contains(integerDatatypes_, type)) {
      parseIntegerConstant(normalizedLiteralContent);
    } else if (type == XSD_BOOLEAN_TYPE) {
      if (normalizedLiteralContent == "true") {
        lastParseResult_ = true;
      } else if (normalizedLiteralContent == "false") {
        lastParseResult_ = false;
      } else if (normalizedLiteralContent == "1") {
        lastParseResult_ = Id::makeBoolFromZeroOrOne(true);
      } else if (normalizedLiteralContent == "0") {
        lastParseResult_ = Id::makeBoolFromZeroOrOne(false);
      } else {
        raiseOrIgnoreTriple(absl::StrCat("Invalid boolean literal: '",
                                         normalizedLiteralContent, "'"));
      }
    } else if (ad_utility::contains(floatDatatypes_, type)) {
      parseDoubleConstant(normalizedLiteralContent);
    } else if (type == XSD_DATETIME_TYPE) {
      lastParseResult_ =
          DateYearOrDuration::parseXsdDatetime(normalizedLiteralContent);
    } else if (type == XSD_DATE_TYPE) {
      lastParseResult_ =
          DateYearOrDuration::parseXsdDate(normalizedLiteralContent);
    } else if (type == XSD_GYEARMONTH_TYPE) {
      lastParseResult_ =
          DateYearOrDuration::parseGYearMonth(normalizedLiteralContent);
    } else if (type == XSD_GYEAR_TYPE) {
      lastParseResult_ =
          DateYearOrDuration::parseGYear(normalizedLiteralContent);
    } else if (type == XSD_DAYTIME_DURATION_TYPE) {
      lastParseResult_ =
          DateYearOrDuration::parseXsdDayTimeDuration(normalizedLiteralContent);
    } else if (type == GEO_WKT_LITERAL) {
      // Not all WKT literals represent points (we can only fold points)
      auto geopoint = GeoPoint::parseFromLiteral(literal, false);
      if (geopoint.has_value()) {
        lastParseResult_ = geopoint.value();
      } else {
        literal.addDatatype(typeIri);
        lastParseResult_ = std::move(literal);
      }
    } else {
      literal.addDatatype(typeIri);
      lastParseResult_ = std::move(literal);
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
    raise(e.what());
  }
  return lastParseResult_;
}

// _____________________________________________________________________________
template <class Tokenizer_T>
void TurtleParser<Tokenizer_T>::raiseDisallowedPrefixOrBaseError() const {
  AD_CORRECTNESS_CHECK(useSimplifiedGrammar_);
  raise(
      "@prefix or @base directives need to be at the beginning of the file "
      "when using the parallel parser. Later redundant redefinitions are "
      "fine. Use '--parallel-parsing false' if you can't guarantee this. If "
      "the reason for this error is that the input is a concatenation of "
      "Turtle files, each of which has the prefixes at the beginning, you "
      "should feed the files to QLever separately instead of concatenated");
}

// _____________________________________________________________________________
template <class Tokenizer_T>
void TurtleParser<Tokenizer_T>::setPrefixOrThrow(
    const std::string& key, const ad_utility::triple_component::Iri& prefix) {
  if (useSimplifiedGrammar_ &&
      (!prefixMap_.contains(key) || prefixMap_[key] != prefix)) {
    raiseDisallowedPrefixOrBaseError();
  }
  prefixMap_[key] = prefix;
}

// _____________________________________________________________________________
template <class Tokenizer_T>
void TurtleParser<Tokenizer_T>::setBaseIriOrThrow(
    const ad_utility::triple_component::Iri& iri) {
  qlever::util::ParsedUri uri{asStringViewUnsafe(iri.getContent())};
  if (useSimplifiedGrammar_ &&
      (!baseIri_.has_value() || baseIri_.value() != uri)) {
    raiseDisallowedPrefixOrBaseError();
  }
  baseIri_ = std::move(uri);
}

// ______________________________________________________________________
template <class T>
TripleComponent TurtleParser<T>::literalAndDatatypeToTripleComponent(
    std::string_view normalizedLiteralContent,
    const TripleComponent::Iri& typeIri,
    const EncodedIriManager& encodedIriManager) {
  RdfStringParser<TurtleParser<T>> parser{&encodedIriManager};

  return parser.literalAndDatatypeToTripleComponentImpl(
      normalizedLiteralContent, typeIri);
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
  AD_CORRECTNESS_CHECK(!allowMultilineLiterals || !useSimplifiedGrammar_);
  // manually parse strings for efficiency
  auto view = tok_.view();
  size_t startPos = 0;
  size_t endPos = 1;
  static constexpr std::array<std::string_view, 4> quotes{R"(""")", R"(''')",
                                                          "\"", "\'"};
  bool foundString = false;
  for (const auto& q : quotes) {
    if (ql::starts_with(view, q)) {
      foundString = true;
      startPos = q.size();
      if (!allowMultilineLiterals && q.size() > 1) {
        if (useSimplifiedGrammar_) {
          raise(
              "Found a multiline string literal with the parallel parser. This "
              "is not supported. Please use `--parallel-parsing false` or "
              "remove the multiline string literal.");
        }
        return false;
      }
      endPos = view.find(q, startPos);
      while (endPos != std::string::npos) {
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
  if (endPos == std::string::npos) {
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
  auto res = iriref() || prefixedName();
  if (!res) {
    return res;
  }
  const auto& s = lastParseResult_.getIri().toStringRepresentation();
  auto optId = encodedIriManager().encode(s);
  if (optId.has_value()) {
    lastParseResult_ = optId.value();
  }
  return res;
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
    triples_.push_back(
        {activeSubject_, activePredicate_, lastParseResult_, defaultGraphIri_});
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
    // constructor. We use `fileBlankNodePrefix_` to ensure that blank nodes
    // with the same label from different files are treated as different, but
    // blank nodes with the same label from the same file (even when parsed in
    // parallel) are treated as the same.
    lastParseResult_ =
        BlankNode{false, absl::StrCat(fileBlankNodePrefix_, "_",
                                      lastParseResult_.getString().substr(2))}
            .toSparql();
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
  if (pos == std::string::npos) {
    return false;
  }
  // these can also be part of a collection etc.
  // find any character that can end a pnameLn when assuming that no
  // escape sequences were used
  auto posEnd = view.find_first_of(" \t\r\n,;", pos);
  if (posEnd == std::string::npos) {
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
  if (!ql::starts_with(view, '<')) {
    return false;
  }
  auto endPos = view.find_first_of("<>\"\n", 1);
  if (endPos == std::string::npos || view[endPos] != '>') {
    raise(
        "Unterminated IRI reference (found '<' but no '>' before "
        "one of the following characters: <, \", newline)");
  }

  auto resolveIri = [this](std::string_view iri) {
    if (baseIri_.has_value()) {
      lastParseResult_ =
          TripleComponent::Iri::fromIrirefConsiderBase(iri, baseIri_.value());
    } else {
      lastParseResult_ = TripleComponent::Iri::fromIriref(iri);
    }
  };
  // In relaxed mode, that is all we check. Otherwise, we check if the IRI is
  // standard-compliant. If not, we output a warning and try to parse it in a
  // more relaxed way.
  if constexpr (T::UseRelaxedParsing) {
    tok_.remove_prefix(endPos + 1);
    resolveIri(view.substr(0, endPos + 1));
    return true;
  } else {
    if (!parseTerminal<TurtleTokenId::Iriref>()) {
      AD_LOG_WARN << "IRI ref not standard-compliant: "
                  << view.substr(0, endPos + 1) << std::endl;
      if (!parseTerminal<TurtleTokenId::IrirefRelaxed>()) {
        return false;
      }
    }
    resolveIri(lastParseResult_.getString());
    return true;
  }
}

// ____________________________________________________________________________
// Helper: synchronously drain one `asyncGetNextBatch` call by blocking the
// calling thread on a promise/future pair. Exceptions surfaced through the
// async callback are rethrown on the caller's thread.
template <typename AsyncParser>
static std::optional<std::vector<TurtleTriple>> drainOneBatch(
    AsyncParser& parser) {
  using Batch = std::optional<std::vector<TurtleTriple>>;
  std::promise<std::pair<std::exception_ptr, Batch>> promise;
  auto future = promise.get_future();
  parser.asyncGetNextBatch(
      [&promise](std::exception_ptr ep, Batch opt) mutable {
        promise.set_value({ep, std::move(opt)});
      });
  auto [ep, opt] = future.get();
  if (ep) std::rethrow_exception(ep);
  return opt;
}

// ____________________________________________________________________________
template <typename T>
RdfStreamParser<T>::RdfStreamParser(const EncodedIriManager* ev)
    : RdfParserBase{ev} {}

// ____________________________________________________________________________
template <typename T>
RdfStreamParser<T>::RdfStreamParser(
    std::unique_ptr<qlever::parser::AsyncBlockSource> rawBuffer,
    const EncodedIriManager* ev, TripleComponent defaultGraphIri)
    : RdfParserBase{ev},
      pool_{std::in_place, NUM_PARALLEL_PARSER_THREADS},
      parser_{qlever::parser::makeStreamingParser<T>(std::move(rawBuffer), ev,
                                                     std::move(defaultGraphIri),
                                                     pool_->get_executor())} {}

// ____________________________________________________________________________
template <typename T>
RdfStreamParser<T>::RdfStreamParser(size_t blocksize,
                                    const std::string& filename,
                                    const EncodedIriManager* ev,
                                    TripleComponent defaultGraphIri)
    : RdfParserBase{ev},
      pool_{std::in_place, NUM_PARALLEL_PARSER_THREADS},
      parser_{qlever::parser::makeStreamingParser<T>(
          std::make_unique<qlever::parser::AsyncFileBlockSource>(
              pool_->get_executor(), blocksize, filename),
          ev, std::move(defaultGraphIri), pool_->get_executor())} {}

// ____________________________________________________________________________
template <typename T>
bool RdfStreamParser<T>::getLineImpl(TurtleTriple* triple) {
  if (cursor_ < buffer_.size()) {
    *triple = std::move(buffer_[cursor_++]);
    return true;
  }
  if (eof_) return false;
  if (!parser_) {
    eof_ = true;
    return false;
  }
  auto batch = drainOneBatch(*parser_);
  if (!batch) {
    eof_ = true;
    return false;
  }
  buffer_ = std::move(*batch);
  cursor_ = 0;
  if (buffer_.empty()) return getLineImpl(triple);
  *triple = std::move(buffer_[cursor_++]);
  return true;
}

// ____________________________________________________________________________
template <typename T>
std::optional<std::vector<TurtleTriple>> RdfStreamParser<T>::getBatch() {
  if (eof_ || !parser_) return std::nullopt;
  return drainOneBatch(*parser_);
}

// ____________________________________________________________________________
template <typename T>
RdfStreamParser<T>::~RdfStreamParser() {
  if (!parser_) return;
  parser_->cancel();
  while (!eof_) {
    try {
      auto batch = drainOneBatch(*parser_);
      if (!batch) eof_ = true;
    } catch (...) {
    }
  }
  // pool_ is destroyed last (joining threads after the destructor body drains
  // the parser).
}

// ____________________________________________________________________________
template <typename T>
RdfParallelParser<T>::RdfParallelParser(const EncodedIriManager* ev)
    : RdfParserBase{ev} {}

// ____________________________________________________________________________
template <typename T>
RdfParallelParser<T>::RdfParallelParser(
    std::unique_ptr<qlever::parser::AsyncBlockSource> rawBuffer,
    const EncodedIriManager* ev, const TripleComponent& defaultGraphIri,
    std::chrono::milliseconds /*sleepTimeForTesting*/)
    : RdfParserBase{ev},
      pool_{std::in_place, NUM_PARALLEL_PARSER_THREADS},
      parser_{qlever::parser::makeParallelFileParser<T>(
          std::move(rawBuffer), ev, defaultGraphIri, pool_->get_executor())} {}

// ____________________________________________________________________________
template <typename T>
RdfParallelParser<T>::RdfParallelParser(size_t blocksize,
                                        const std::string& filename,
                                        const EncodedIriManager* ev,
                                        const TripleComponent& defaultGraphIri)
    : RdfParserBase{ev},
      pool_{std::in_place, NUM_PARALLEL_PARSER_THREADS},
      parser_{qlever::parser::makeParallelFileParser<T>(
          std::make_unique<qlever::parser::AsyncFileBlockSource>(
              pool_->get_executor(), blocksize, filename),
          ev, defaultGraphIri, pool_->get_executor())} {}

// ____________________________________________________________________________
template <typename T>
bool RdfParallelParser<T>::getLineImpl(TurtleTriple* triple) {
  if (cursor_ < buffer_.size()) {
    *triple = std::move(buffer_[cursor_++]);
    return true;
  }
  if (eof_) return false;
  if (!parser_) {
    eof_ = true;
    return false;
  }
  auto batch = drainOneBatch(*parser_);
  if (!batch) {
    eof_ = true;
    return false;
  }
  buffer_ = std::move(*batch);
  cursor_ = 0;
  if (buffer_.empty()) return getLineImpl(triple);
  *triple = std::move(buffer_[cursor_++]);
  return true;
}

// ____________________________________________________________________________
template <typename T>
std::optional<std::vector<TurtleTriple>> RdfParallelParser<T>::getBatch() {
  if (eof_ || !parser_) return std::nullopt;
  return drainOneBatch(*parser_);
}

// ____________________________________________________________________________
template <typename T>
RdfParallelParser<T>::~RdfParallelParser() {
  if (!parser_) return;
  parser_->cancel();
  while (!eof_) {
    try {
      auto batch = drainOneBatch(*parser_);
      if (!batch) eof_ = true;
    } catch (...) {
    }
  }
}

// ____________________________________________________________________________
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

// ____________________________________________________________________________
RdfMultifileParser::RdfMultifileParser(
    const EncodedIriManager* encodedIriManager)
    : RdfParserBase{encodedIriManager} {}

// ____________________________________________________________________________
RdfMultifileParser::RdfMultifileParser(
    const std::vector<qlever::InputFileSpecification>& files,
    const EncodedIriManager* encodedIriManager,
    ad_utility::MemorySize bufferSize)
    : RdfParserBase{encodedIriManager},
      pool_{std::in_place, NUM_PARALLEL_PARSER_THREADS},
      parser_{std::make_unique<qlever::parser::AsyncMultifileParser>(
          files, encodedIriManager, bufferSize, pool_->get_executor())} {}

// ____________________________________________________________________________
bool RdfMultifileParser::getLineImpl(TurtleTriple*) { AD_FAIL(); }

// ____________________________________________________________________________
std::optional<std::vector<TurtleTriple>> RdfMultifileParser::getBatch() {
  if (eof_ || !parser_) return std::nullopt;
  return drainOneBatch(*parser_);
}

// ____________________________________________________________________________
RdfMultifileParser::~RdfMultifileParser() {
  if (!parser_) return;
  parser_->cancel();
  while (!eof_) {
    try {
      auto batch = drainOneBatch(*parser_);
      if (!batch) eof_ = true;
    } catch (...) {
    }
  }
}

// Explicit instantiations.
template class TurtleParser<Tokenizer>;
template class TurtleParser<TokenizerCtre>;
template class NQuadParser<Tokenizer>;
template class NQuadParser<TokenizerCtre>;
template class RdfStreamParser<TurtleParser<Tokenizer>>;
template class RdfStreamParser<TurtleParser<TokenizerCtre>>;
template class RdfStreamParser<NQuadParser<Tokenizer>>;
template class RdfStreamParser<NQuadParser<TokenizerCtre>>;
template class RdfParallelParser<TurtleParser<Tokenizer>>;
template class RdfParallelParser<TurtleParser<TokenizerCtre>>;
template class RdfParallelParser<NQuadParser<Tokenizer>>;
template class RdfParallelParser<NQuadParser<TokenizerCtre>>;
