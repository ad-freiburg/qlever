// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/LoadURL.h"

#include "util/http/HttpUtils.h"

// _____________________________________________________________________________
string LoadURL::getCacheKeyImpl() const {
  // TODO<qup42> Allow some kind of caching: controlled by a runtime
  // parameter or based on HTTP headers like ETag or Last-Modified.
  return absl::StrCat("LOAD URL ", loadURLClause_.url_.asString(), " ",
                      loadURLClause_.silent_ ? "SILENT " : "", cacheBreaker_);
}

// _____________________________________________________________________________
string LoadURL::getDescriptor() const {
  return absl::StrCat("LOAD URL ", loadURLClause_.url_.asString());
}

// _____________________________________________________________________________
size_t LoadURL::getResultWidth() const { return 4; }

// _____________________________________________________________________________
size_t LoadURL::getCostEstimate() {
  // TODO: For now, we don't have any information about the cost at query
  // planning time, so we just return ten times the estimated size.
  return 10 * getSizeEstimateBeforeLimit();
}

// _____________________________________________________________________________
uint64_t LoadURL::getSizeEstimateBeforeLimit() {
  // TODO: For now, we don't have any information about the result size at
  // query planning time, so we just return `100'000`.
  return 100'000;
}

// _____________________________________________________________________________
float LoadURL::getMultiplicity(size_t) {
  // TODO: For now, we don't have any information about the multiplicities at
  // query planning time, so we just return `1` for each column.
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
  // TODO<qup42> implement lazy loading; this requires some modifications to the
  // parse
  LOG(INFO) << "Loading RDF dataset from " << loadURLClause_.url_.asString()
            << std::endl;
  HttpOrHttpsResponse response =
      getResultFunction_(loadURLClause_.url_, cancellationHandle_,
                         boost::beast::http::verb::get, "", "", "");

  auto throwErrorWithContext = [this, &response](std::string_view sv) {
    std::string ctx;
    ctx.reserve(100);
    for (const auto& bytes : std::move(response.body_)) {
      ctx += std::string(reinterpret_cast<const char*>(bytes.data()),
                         bytes.size());
      if (ctx.size() >= 100) {
        break;
      }
    }
    this->throwErrorWithContext(sv, std::string_view(ctx).substr(0, 100));
  };

  if (response.status_ != boost::beast::http::status::ok) {
    throwErrorWithContext(absl::StrCat(
        "RDF dataset responded with HTTP status code: ",
        static_cast<int>(response.status_), ", ",
        toStd(boost::beast::http::obsolete_reason(response.status_))));
  }
  std::optional<ad_utility::MediaType> mediaType =
      ad_utility::toMediaType(response.contentType_);
  if (!mediaType) {
    throwErrorWithContext(
        "QLever requires the endpoint of a LoadURL to return the mediatype.");
  }
  if (!ad_utility::contains(SUPPORTED_MEDIATYPES, mediaType.value())) {
    throwErrorWithContext(
        absl::StrCat("Unsupported media type \"", toString(mediaType.value()),
                     "\". Supported media types are \"text/turtle\" and "
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
    result(row_idx, 3) = defaultGraph_;
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
  map[Variable("?g")] = makeAlwaysDefinedColumn(3);
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
