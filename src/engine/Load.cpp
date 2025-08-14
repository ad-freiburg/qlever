// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/Load.h"

#include "global/RuntimeParameters.h"
#include "util/http/HttpUtils.h"

// _____________________________________________________________________________
Load::Load(QueryExecutionContext* qec, parsedQuery::Load loadClause,
           SendRequestType getResultFunction)
    : Operation(qec),
      loadClause_(std::move(loadClause)),
      getResultFunction_(std::move(getResultFunction)),
      loadResultCachingEnabled_(
          RuntimeParameters().get<"cache-load-results">()) {}

// _____________________________________________________________________________
std::string Load::getCacheKeyImpl() const {
  if (RuntimeParameters().get<"cache-load-results">()) {
    return absl::StrCat("LOAD ", loadClause_.iri_.toStringRepresentation(),
                        loadClause_.silent_ ? " SILENT" : "");
  }
  return absl::StrCat("LOAD ", cacheBreaker_);
}

// _____________________________________________________________________________
std::string Load::getDescriptor() const {
  return absl::StrCat("LOAD ", loadClause_.iri_.toStringRepresentation());
}

// _____________________________________________________________________________
size_t Load::getResultWidth() const { return 3; }

// _____________________________________________________________________________
size_t Load::getCostEstimate() {
  // This Operation will always be the only operation in a query tree, so we
  // don't really need estimates.
  return 10 * getSizeEstimateBeforeLimit();
}

// _____________________________________________________________________________
uint64_t Load::getSizeEstimateBeforeLimit() {
  // This Operation will always be the only operation in a query tree, so we
  // don't really need estimates.
  return 100'000;
}

// _____________________________________________________________________________
float Load::getMultiplicity(size_t) {
  // This Operation will always be the only operation in a query tree, so we
  // don't really need estimates.
  return 1;
}

// _____________________________________________________________________________
bool Load::knownEmptyResult() { return false; }

// _____________________________________________________________________________
std::unique_ptr<Operation> Load::cloneImpl() const {
  return std::make_unique<Load>(_executionContext, loadClause_);
}

// _____________________________________________________________________________
std::vector<ColumnIndex> Load::resultSortedOn() const { return {}; }

// _____________________________________________________________________________
Result Load::computeResult(bool requestLaziness) {
  auto makeSilentResult = [this]() -> Result {
    return {IdTable{getResultWidth(), getExecutionContext()->getAllocator()},
            resultSortedOn(), LocalVocab{}};
  };

  // In the syntax test mode we don't even try to compute the result, as this
  // could run into timeouts which would be a waste of time and is hard to
  // properly recover from.
  if (RuntimeParameters().get<"syntax-test-mode">()) {
    return makeSilentResult();
  }
  try {
    return computeResultImpl(requestLaziness);
  } catch (const ad_utility::CancellationException&) {
    throw;
  } catch (const ad_utility::detail::AllocationExceedsLimitException&) {
    throw;
  } catch (const std::exception&) {
    // If the `SILENT` keyword is set, catch the error and return the neutral
    // element for this operation (an empty `IdTable`). The `IdTable` is used
    // to fill in the variables in the template triple `?s ?p ?o`. The empty
    // `IdTable` results in no triples being updated.
    if (loadClause_.silent_) {
      return makeSilentResult();
    } else {
      throw;
    }
  }
}

// _____________________________________________________________________________
Result Load::computeResultImpl([[maybe_unused]] bool requestLaziness) {
  // TODO<qup42> implement lazy loading; requires modifications to the parser
  ad_utility::httpUtils::Url url{
      asStringViewUnsafe(loadClause_.iri_.getContent())};
  LOG(INFO) << "Loading RDF dataset from " << url.asString() << std::endl;
  HttpOrHttpsResponse response = getResultFunction_(
      url, cancellationHandle_, boost::beast::http::verb::get, "", "", "");

  auto throwErrorWithContext = [this, &response](std::string_view sv) {
    this->throwErrorWithContext(sv, std::move(response).readResponseHead(100));
  };

  if (response.status_ != boost::beast::http::status::ok) {
    throwErrorWithContext(absl::StrCat(
        "RDF dataset responded with HTTP status code: ",
        static_cast<int>(response.status_), ", ",
        toStd(boost::beast::http::obsolete_reason(response.status_))));
  }
  if (response.contentType_.empty()) {
    throwErrorWithContext(
        "QLever requires the `Content-Type` header to be set for the HTTP "
        "response.");
  }
  // If the `Content-Type` is not one of the media types known to QLever, then
  // std::nullopt is returned by `toMediaType`.
  std::optional<ad_utility::MediaType> mediaType =
      ad_utility::toMediaType(response.contentType_);
  static const auto supportedMediatypes = ad_utility::lazyStrJoin(
      ql::views::transform(
          SUPPORTED_MEDIATYPES,
          [](const ad_utility::MediaType& mt) { return toString(mt); }),
      ", ");
  if (!mediaType ||
      !ad_utility::contains(SUPPORTED_MEDIATYPES, mediaType.value())) {
    throwErrorWithContext(absl::StrCat(
        "Unsupported `Content-Type` of response: \"", response.contentType_,
        "\". Supported `Content-Type`s are ", supportedMediatypes));
  }
  using Re2Parser = RdfStringParser<TurtleParser<Tokenizer>>;
  const auto& encodedIriManager = getIndex().encodedIriManager();
  auto parser = Re2Parser(&encodedIriManager);
  std::string body;
  for (const auto& bytes : response.body_) {
    body.append(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  }
  parser.setInputStream(body);
  LocalVocab lv;
  IdTable result{getResultWidth(), getExecutionContext()->getAllocator()};
  auto toId = [this, &lv, &encodedIriManager](TripleComponent&& tc) {
    return std::move(tc).toValueId(getIndex().getVocab(), lv,
                                   encodedIriManager);
  };
  for (auto& triple : parser.parseAndReturnAllTriples()) {
    result.push_back(
        std::array{toId(std::move(triple.subject_)),
                   toId(TripleComponent(std::move(triple.predicate_))),
                   toId(std::move(triple.object_))});
    checkCancellation();
  }
  return Result{std::move(result), resultSortedOn(), std::move(lv)};
}

// _____________________________________________________________________________
VariableToColumnMap Load::computeVariableToColumnMap() const {
  VariableToColumnMap map;
  map[Variable("?s")] = makeAlwaysDefinedColumn(0);
  map[Variable("?p")] = makeAlwaysDefinedColumn(1);
  map[Variable("?o")] = makeAlwaysDefinedColumn(2);
  return map;
}

// _____________________________________________________________________________
void Load::throwErrorWithContext(std::string_view msg,
                                 std::string_view first100,
                                 std::string_view last100) const {
  throw std::runtime_error(absl::StrCat(
      "Error while executing a Load request to <",
      loadClause_.iri_.toStringRepresentation(), ">: ", msg,
      ". First 100 bytes of the response: '", first100,
      (last100.empty() ? "'"
                       : absl::StrCat(", last 100 bytes: '", last100, "'"))));
}

// _____________________________________________________________________________
bool Load::canResultBeCachedImpl() const { return loadResultCachingEnabled_; }

// _____________________________________________________________________________
void Load::resetGetResultFunctionForTesting(SendRequestType func) {
  getResultFunction_ = std::move(func);
}
