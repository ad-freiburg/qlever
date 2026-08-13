// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleInstantiator.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/ConstructDeduplicator.h"
#include "global/Constants.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"
#include "util/Views.h"

namespace qlever::constructExport {

// _____________________________________________________________________________
std::optional<EvaluatedTerm> instantiateTerm(
    const PreprocessedTerm& term, const BatchEvaluationResult& batchResult,
    size_t rowIdxInBatch, size_t rowIdxTotal) {
  return std::visit(
      [&](const auto& t) -> std::optional<EvaluatedTerm> {
        using T = std::decay_t<decltype(t)>;

        if constexpr (std::is_same_v<T, PrecomputedConstant>) {
          return t.evaluatedTerm_;
        } else if constexpr (std::is_same_v<T, PrecomputedVariable>) {
          return batchResult.getVariable(t.columnIndex_, rowIdxInBatch);
        } else if constexpr (std::is_same_v<T, PrecomputedBlankNode>) {
          return std::make_shared<const EvaluatedTermData>(EvaluatedTermData{
              absl::StrCat(t.prefix_, rowIdxTotal, t.suffix_), nullptr});
        } else {
          static_assert(ad_utility::alwaysFalse<T>, "Unhandled variant type");
        }
      },
      term);
}

namespace {
// Instantiates one template triple for one result row, or returns
// `nullopt` if a term is undefined or the triple is a duplicate under
// `deduplication`.
std::optional<EvaluatedTriple> tryInstantiateTriple(
    const PreprocessedTriple& triple, const BatchEvaluationResult& batchResult,
    size_t rowInBatch, size_t blankNodeRowId, size_t tripleIdx,
    const PreprocessedConstructTemplate& tmpl,
    const std::optional<DeduplicationParams>& deduplication) {
  auto instantiate = [&triple, &batchResult, rowInBatch,
                      blankNodeRowId](size_t pos) {
    return instantiateTerm(triple.at(pos), batchResult, rowInBatch,
                           blankNodeRowId);
  };
  auto subject = instantiate(0);
  auto predicate = instantiate(1);
  auto object = instantiate(2);
  if (!subject || !predicate || !object) {
    return std::nullopt;
  }
  if (deduplication) {
    const size_t rowIdxInIdTable =
        deduplication.value().ctx_.get().firstRow_ + rowInBatch;
    if (!deduplication.value().deduplicator_.get().isNew(
            tripleIdx, rowIdxInIdTable, tmpl, deduplication.value().ctx_)) {
      return std::nullopt;
    }
  }
  return EvaluatedTriple{*subject, *predicate, *object};
}
}  // namespace

// _____________________________________________________________________________
std::vector<EvaluatedTriple> instantiateBatch(
    const PreprocessedConstructTemplate& tmpl,
    const BatchEvaluationResult& batchResult, size_t batchOffset,
    std::optional<DeduplicationParams> deduplicationParams) {
  std::vector<EvaluatedTriple> triples;
  triples.reserve(batchResult.numRows_ * tmpl.preprocessedTriples_.size());

  for (const size_t rowInBatch :
       ad_utility::integerRange(batchResult.numRows_)) {
    const size_t blankNodeRowId = batchOffset + rowInBatch;
    for (auto&& [tripleIdx, triple] :
         ::ranges::views::enumerate(tmpl.preprocessedTriples_)) {
      if (auto instantiated = tryInstantiateTriple(
              triple, batchResult, rowInBatch, blankNodeRowId,
              static_cast<size_t>(tripleIdx), tmpl, deduplicationParams)) {
        triples.push_back(std::move(*instantiated));
      }
    }
  }
  return triples;
}

// _____________________________________________________________________________
std::string formatTerm(const EvaluatedTermData& term, bool includeDataType) {
  if (term.rdfTermDataType_ == nullptr) {
    // IRI, blank node, or vocab-indexed literal: already in final form.
    return term.rdfTermString_;
  }
  const auto* i = static_cast<const char*>(XSD_INT_TYPE);
  const auto* d = static_cast<const char*>(XSD_DECIMAL_TYPE);
  const auto* b = static_cast<const char*>(XSD_BOOLEAN_TYPE);

  // Note: XSD_DOUBLE_TYPE values (for example "NaN", "INF", "-INF") always
  // include the datatype.
  if (!includeDataType &&
      (term.rdfTermDataType_ == i || term.rdfTermDataType_ == d ||
       (term.rdfTermDataType_ == b && term.rdfTermString_.length() > 1))) {
    return term.rdfTermString_;
  }
  return absl::StrCat("\"", term.rdfTermString_, "\"^^<", term.rdfTermDataType_,
                      ">");
}

namespace {
// Returns true iff `formatTerm` (and `appendFormattedTerm`) emit `term` in the
// fully qualified form `"value"^^<datatype>` as opposed to the short form
// without quotation marks.
bool needsDatatypeWrapper(const EvaluatedTermData& term, bool includeDataType) {
  if (term.rdfTermDataType_ == nullptr) {
    return false;
  }
  const auto* i = static_cast<const char*>(XSD_INT_TYPE);
  const auto* d = static_cast<const char*>(XSD_DECIMAL_TYPE);
  const auto* b = static_cast<const char*>(XSD_BOOLEAN_TYPE);
  // Note: XSD_DOUBLE_TYPE values (for example "NaN", "INF", "-INF") always
  // include the datatype.
  return includeDataType ||
         (term.rdfTermDataType_ != i && term.rdfTermDataType_ != d &&
          !(term.rdfTermDataType_ == b && term.rdfTermString_.length() > 1));
}

// Appends the formatted term to `out`, with the same semantics as
// `formatTerm`, but without materializing an intermediate string.
void appendFormattedTerm(std::string& out, const EvaluatedTermData& term,
                         bool includeDataType) {
  if (!needsDatatypeWrapper(term, includeDataType)) {
    // IRI, blank node, vocab-indexed literal, or encoded value in its short
    // form: the string is already in its final form.
    out.append(term.rdfTermString_);
    return;
  }
  out.push_back('"');
  out.append(term.rdfTermString_);
  out.append("\"^^<");
  out.append(term.rdfTermDataType_);
  out.push_back('>');
}

// Returns true iff `RdfEscaping::validRDFLiteralFromNormalized` would have to
// escape `normLiteral`, i.e. if there are characters between its first and
// last quote that must be escaped.
bool needsRDFLiteralEscaping(std::string_view normLiteral) {
  AD_CONTRACT_CHECK(ql::starts_with(normLiteral, '"'));
  size_t posSecondQuote = normLiteral.find('"', 1);
  AD_CONTRACT_CHECK(posSecondQuote != std::string::npos);
  size_t posLastQuote = normLiteral.rfind('"');
  // If there are only two quotes (the first and the last, which every
  // normalized literal has) and no escape sequences, there is nothing to do.
  return posSecondQuote != posLastQuote ||
         normLiteral.find_first_of("\\\n\r") != std::string::npos;
}

// Appends `normLiteral` to `out`, escaping it exactly like
// `RdfEscaping::validRDFLiteralFromNormalized` does, but without allocating a
// new string in the common case where no escaping is required.
void appendValidRDFLiteral(std::string& out, std::string_view normLiteral) {
  if (!needsRDFLiteralEscaping(normLiteral)) {
    out.append(normLiteral);
    return;
  }
  // Otherwise escape first all backslashes then all quotes (the order is
  // important) in the part between the first and the last quote and leave the
  // rest unchanged.
  size_t posLastQuote = normLiteral.rfind('"');
  std::string_view normalizedContent = normLiteral.substr(1, posLastQuote - 1);
  out.push_back('"');
  using Replacement = std::pair<absl::string_view, absl::string_view>;
  absl::StrAppend(
      &out, absl::StrReplaceAll(
                normalizedContent,
                {Replacement{R"(\\)", R"(\\\\)"}, Replacement{"\n", "\\n"},
                 Replacement{"\r", "\\r"}, Replacement{R"(")", R"(\")"}}));
  out.append(normLiteral.substr(posLastQuote));
}

// Appends `input` to `out`, escaping it exactly like
// `RdfEscaping::escapeForCsv` does.
void appendEscapedForCsv(std::string& out, std::string_view input) {
  if (input.find_first_of("\"\r\n,") == std::string_view::npos) [[likely]] {
    out.append(input);
    return;
  }
  out.push_back('"');
  using Replacement = std::pair<absl::string_view, absl::string_view>;
  absl::StrAppend(&out,
                  absl::StrReplaceAll(input, {Replacement{"\"", "\"\""}}));
  out.push_back('"');
}

// Appends `input` to `out`, escaping it exactly like
// `RdfEscaping::escapeForTsv` does.
void appendEscapedForTsv(std::string& out, std::string_view input) {
  if (input.find_first_of("\n\t") == std::string_view::npos) [[likely]] {
    out.append(input);
    return;
  }
  using Replacement = std::pair<absl::string_view, absl::string_view>;
  absl::StrAppend(&out, absl::StrReplaceAll(input, {Replacement{"\t", " "},
                                                    Replacement{"\n", "\\n"}}));
}
}  // namespace

// _____________________________________________________________________________
std::string formatTriple(const EvaluatedTriple& evaluatedTriple,
                         const ad_utility::MediaType& format) {
  using enum ad_utility::MediaType;
  static constexpr std::array supportedFormats{turtle, csv, tsv, ntriples};
  AD_CONTRACT_CHECK(ad_utility::contains(supportedFormats, format));

  const auto& [subject, predicate, object] = evaluatedTriple;

  const bool includeDataType = (format == ntriples);

  // Buffers that are reused across rows: their capacity is retained between
  // calls, so rows that are not larger than the previous maximum do not
  // trigger any allocation. The only per-row allocation is the copy of the
  // assembled row into the return value (which the caller owns). `scratch`
  // is only used to hold the formatted object term (turtle/ntriples) or the
  // formatted terms (csv/tsv) before they are appended (and possibly escaped).
  thread_local std::string buffer;
  thread_local std::string scratch;
  buffer.clear();
  scratch.clear();

  const EvaluatedTermData& s = *subject;
  const EvaluatedTermData& p = *predicate;
  const EvaluatedTermData& o = *object;

  // Conservative estimate of the size of the assembled row. It never
  // under-allocates: an escape sequence replaces a single character by at
  // most two characters, so reserving an extra copy of the terms that may be
  // escaped is always sufficient. The over-allocation is bounded by that
  // escape headroom (plus a small constant for the separators), and the
  // capacity is retained in the thread-local buffer for subsequent rows.
  auto formattedTermSize = [&includeDataType](const EvaluatedTermData& term) {
    size_t size = term.rdfTermString_.size();
    if (needsDatatypeWrapper(term, includeDataType)) {
      size += 6 + std::char_traits<char>::length(term.rdfTermDataType_);
    }
    return size;
  };
  // The formatted object is a literal (and thus subject to escaping) iff it
  // either carries a datatype wrapper (which always starts with a quote) or
  // its raw string already starts with a quote.
  const bool objectIsLiteral = needsDatatypeWrapper(o, includeDataType) ||
                               ql::starts_with(o.rdfTermString_, '"');
  size_t estimatedSize =
      formattedTermSize(s) + formattedTermSize(p) + formattedTermSize(o);
  if (format == turtle || format == ntriples) {
    // Only the object can be escaped, and only if it is a literal.
    if (objectIsLiteral) {
      estimatedSize += formattedTermSize(o);
    }
    estimatedSize += 5;  // two separating spaces + " .\n"
  } else {
    // All three terms can be escaped.
    estimatedSize +=
        formattedTermSize(s) + formattedTermSize(p) + formattedTermSize(o);
    estimatedSize += 3;  // two separators + trailing newline
  }
  buffer.reserve(estimatedSize);

  if (format == turtle || format == ntriples) {
    // Only escape literals (strings starting with "). IRIs and blank nodes
    // are used as-is, avoiding an unnecessary string copy.
    appendFormattedTerm(buffer, s, includeDataType);
    buffer.push_back(' ');
    appendFormattedTerm(buffer, p, includeDataType);
    buffer.push_back(' ');
    // The object is escaped like a normalized RDF literal if it is a literal.
    // Format it into the scratch buffer first, because the escaping decision
    // depends on the formatted form.
    if (objectIsLiteral) {
      appendFormattedTerm(scratch, o, includeDataType);
      appendValidRDFLiteral(buffer, scratch);
    } else {
      appendFormattedTerm(buffer, o, includeDataType);
    }
    buffer.append(" .\n");
  } else {
    AD_CONTRACT_CHECK(format == csv || format == tsv);
    auto appendEscapedTerm = [&](const EvaluatedTermData& term) {
      scratch.clear();
      appendFormattedTerm(scratch, term, includeDataType);
      if (format == csv) {
        appendEscapedForCsv(buffer, scratch);
      } else {
        appendEscapedForTsv(buffer, scratch);
      }
    };
    appendEscapedTerm(s);
    buffer.push_back(format == csv ? ',' : '\t');
    appendEscapedTerm(p);
    buffer.push_back(format == csv ? ',' : '\t');
    appendEscapedTerm(o);
    buffer.push_back('\n');
  }
  return buffer;
}

// _____________________________________________________________________________
StringTriple createStringTriple(const EvaluatedTriple& evaluatedTriple,
                                bool includeDataType) {
  const auto& [subject, predicate, object] = evaluatedTriple;

  std::string s = formatTerm(*subject, includeDataType);
  std::string p = formatTerm(*predicate, includeDataType);
  std::string o = formatTerm(*object, includeDataType);

  return StringTriple{std::move(s), std::move(p), std::move(o)};
}
}  // namespace qlever::constructExport
