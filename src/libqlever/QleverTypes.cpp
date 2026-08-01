#include "libqlever/QleverTypes.h"

#include "global/RuntimeParameters.h"
#include "util/http/MediaTypes.h"

namespace qlever {
using ad_utility::MediaType;

// _____________________________________________________________________________
void PlannedQuery::adjustParsedQueryLimitOffset(
    const ad_utility::MediaType& mediaType, std::optional<uint64_t> sendLimit) {
  // Read the export limit from the `send` parameter (historical name). This
  // limits the number of bindings exported in `ExportQueryExecutionTrees`.
  //
  // NOTE: This was originally designed exclusively for `qlever-results+json`.
  // However, when the runtime parameter `sparql-results-json-with-time` is set
  // (which is the default), we now also apply it to `sparql-results+json`.
  auto& limitOffset = parsedQuery_._limitOffset;
  auto& exportLimit = limitOffset.exportLimit_;
  bool considerSendParameter =
      mediaType == MediaType::qleverJson ||
      (getRuntimeParameter<&RuntimeParameters::sparqlResultsJsonWithTime_>() &&
       mediaType == MediaType::sparqlJson);
  if (sendLimit.has_value() && considerSendParameter) {
    exportLimit = std::move(sendLimit);
  }
}

}  // namespace qlever
