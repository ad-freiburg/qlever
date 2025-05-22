// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/LoadURL.h"

#include "global/RuntimeParameters.h"
#include "util/http/HttpUtils.h"

// _____________________________________________________________________________
string LoadURL::getCacheKeyImpl() const {
  // TODO<qup42> do caching based on ETag, Last-Modified or similar
  if (RuntimeParameters().get<"cache-load-results">()) {
    return absl::StrCat("LOAD URL ", loadURLClause_.url_.asString(),
                        loadURLClause_.silent_ ? " SILENT" : "");
  }
  return absl::StrCat("LOAD URL ", cacheBreaker_);
}

// _____________________________________________________________________________
string LoadURL::getDescriptor() const {
  return absl::StrCat("LOAD URL ", loadURLClause_.url_.asString());
}

// _____________________________________________________________________________
size_t LoadURL::getResultWidth() const { return 3; }

// _____________________________________________________________________________
size_t LoadURL::getCostEstimate() {
  // This Operation will always be the only operation in a query tree, so we
  // don't really need estimates.
  return 10 * getSizeEstimateBeforeLimit();
}

// _____________________________________________________________________________
uint64_t LoadURL::getSizeEstimateBeforeLimit() {
  // This Operation will always be the only operation in a query tree, so we
  // don't really need estimates.
  return 100'000;
}

// _____________________________________________________________________________
float LoadURL::getMultiplicity(size_t) {
  // This Operation will always be the only operation in a query tree, so we
  // don't really need estimates.
  return 1;
}

// _____________________________________________________________________________
bool LoadURL::knownEmptyResult() { return false; }

// _____________________________________________________________________________
std::unique_ptr<Operation> LoadURL::cloneImpl() const {
  return std::make_unique<LoadURL>(_executionContext, loadURLClause_);
}

// _____________________________________________________________________________
vector<ColumnIndex> LoadURL::resultSortedOn() const { return {}; }

// _____________________________________________________________________________
Result LoadURL::computeResult(bool requestLaziness) {
  try {
    return computeResultImpl(requestLaziness);
  } catch (const ad_utility::CancellationException&) {
    throw;
  } catch (const ad_utility::detail::AllocationExceedsLimitException&) {
    throw;
  } catch (const std::exception&) {
    // If the `SILENT` keyword is set, catch the error and return a neutral
    // Element.
    if (loadURLClause_.silent_) {
      IdTable idTable{getResultWidth(), getExecutionContext()->getAllocator()};
      Id u = Id::makeUndefined();
      idTable.push_back(std::array{u, u, u});
      return {std::move(idTable), resultSortedOn(), LocalVocab{}};
    }
    throw;
  }
}

// _____________________________________________________________________________
Result LoadURL::computeResultImpl([[maybe_unused]] bool requestLaziness) {
  // TODO<qup42> implement lazy loading; requires modifications to the parser
  LOG(INFO) << "Loading RDF dataset from " << loadURLClause_.url_.asString()
            << std::endl;
  HttpOrHttpsResponse response =
      getResultFunction_(loadURLClause_.url_, cancellationHandle_,
                         boost::beast::http::verb::get, "", "", "");

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
  auto supportedMediatypes = ad_utility::lazyStrJoin(
      ql::views::transform(
          SUPPORTED_MEDIATYPES,
          [](const ad_utility::MediaType& mt) { return toString(mt); }),
      ", ");
  if (!mediaType) {
    throwErrorWithContext(absl::StrCat("Unknown `Content-Type` \"",
                                       response.contentType_,
                                       "\". Supported: ", supportedMediatypes));
  }
  if (!ad_utility::contains(SUPPORTED_MEDIATYPES, mediaType.value())) {
    throwErrorWithContext(absl::StrCat(
        "Unsupported value for `Content-Type` \"", toString(mediaType.value()),
        "\". Supported: ", supportedMediatypes));
  }
  using Re2Parser = RdfStringParser<TurtleParser<Tokenizer>>;
  auto parser = Re2Parser();
  std::string body;
  for (const auto& bytes : response.body_) {
    body.append(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  }
  parser.setInputStream(body);
  LocalVocab lv;
  IdTable result{getResultWidth(), getExecutionContext()->getAllocator()};
  auto toId = [this, &lv](TripleComponent&& tc) {
    return std::move(tc).toValueId(getIndex().getVocab(), lv);
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
VariableToColumnMap LoadURL::computeVariableToColumnMap() const {
  VariableToColumnMap map;
  map[Variable("?s")] = makeAlwaysDefinedColumn(0);
  map[Variable("?p")] = makeAlwaysDefinedColumn(1);
  map[Variable("?o")] = makeAlwaysDefinedColumn(2);
  return map;
}

// _____________________________________________________________________________
void LoadURL::throwErrorWithContext(std::string_view msg,
                                    std::string_view first100,
                                    std::string_view last100) const {
  throw std::runtime_error(absl::StrCat(
      "Error while executing a LoadURL request to <",
      loadURLClause_.url_.asString(), ">: ", msg,
      ". First 100 bytes of the response: '", first100,
      (last100.empty() ? "'"
                       : absl::StrCat(", last 100 bytes: '", last100, "'"))));
}

// _____________________________________________________________________________
bool LoadURL::canResultBeCached() const { return false; }
