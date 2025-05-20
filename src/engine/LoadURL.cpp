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
  auto load = std::make_unique<LoadURL>(_executionContext, loadURLClause_);
  load->cacheBreaker_ = cacheBreaker_;
  return load;
}

// _____________________________________________________________________________
vector<ColumnIndex> LoadURL::resultSortedOn() const { return {}; }

// _____________________________________________________________________________
Result LoadURL::computeResult(bool) {
  // TODO<qup42> implement lazy loading; requires modifications to the parser
  LOG(INFO) << "Loading RDF dataset from " << loadURLClause_.url_.asString()
            << std::endl;
  HttpOrHttpsResponse response =
      getResultFunction_(loadURLClause_.url_, cancellationHandle_,
                         boost::beast::http::verb::get, "", "", "");

  auto throwErrorWithContext = [this, &response](std::string_view sv) {
    this->throwErrorWithContext(sv, readResponseHead(std::move(response)));
  };

  if (response.status_ != boost::beast::http::status::ok) {
    throwErrorWithContext(absl::StrCat(
        "RDF dataset responded with HTTP status code: ",
        static_cast<int>(response.status_), ", ",
        toStd(boost::beast::http::obsolete_reason(response.status_))));
  }
  if (response.contentType_.empty()) {
    throwErrorWithContext(
        "QLever requires the `Content-Type` header to be set for LoadURL.");
  }
  // If the `Content-Type` is not one of the media types known to QLever, then
  // std::nullopt is returned by `toMediaType`.
  std::optional<ad_utility::MediaType> mediaType =
      ad_utility::toMediaType(response.contentType_);
  if (!mediaType) {
    throwErrorWithContext(
        absl::StrCat("Unknown `Content-Type` \"", response.contentType_, "\""));
  }
  if (!ad_utility::contains(SUPPORTED_MEDIATYPES, mediaType.value())) {
    throwErrorWithContext(absl::StrCat(
        "Unsupported value for `Content-Type` \"", toString(mediaType.value()),
        "\". Supported are \"text/turtle\" and "
        "\"application/n-triples\"."));
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
  size_t row_idx = 0;
  for (auto& triple : parser.parseAndReturnAllTriples()) {
    result.emplace_back();
    result(row_idx, 0) =
        std::move(triple.subject_).toValueId(getIndex().getVocab(), lv);
    result(row_idx, 1) = std::move(TripleComponent(triple.predicate_))
                             .toValueId(getIndex().getVocab(), lv);
    result(row_idx, 2) =
        std::move(triple.object_).toValueId(getIndex().getVocab(), lv);
    row_idx++;
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
std::string LoadURL::readResponseHead(HttpOrHttpsResponse response,
                                      size_t contextLength) {
  std::string ctx;
  ctx.reserve(contextLength);
  for (const auto& bytes : std::move(response.body_)) {
    // only copy until the ctx has reached contextLength
    size_t bytesToCopy = std::min(bytes.size(), contextLength - ctx.size());
    ctx += std::string_view(reinterpret_cast<const char*>(bytes.data()),
                            bytesToCopy);
    if (ctx.size() == contextLength) {
      break;
    }
  }
  return ctx;
}
