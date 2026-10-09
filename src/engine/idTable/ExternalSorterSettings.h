// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_IDTABLE_EXTERNALSORTERSETTINGS_H
#define QLEVER_SRC_ENGINE_IDTABLE_EXTERNALSORTERSETTINGS_H

#include <cstddef>
#include <string>

namespace ad_utility::compressedExternalIdTable {

// The runtime parameters that configure the external sorters (see
// `CompressedExternalIdTable.h`), see
// `RuntimeParameters::externalSorterCompressionLevel_`,
// `RuntimeParameters::mergePhaseMaxChunksInFlight_` and
// `RuntimeParameters::mergePhaseMaxOutputBlockRows_` for their meaning.
struct ExternalSorterSettings {
  std::string compressionLevel_;
  size_t mergePhaseMaxChunksInFlight_;
  size_t mergePhaseMaxOutputBlockRows_;
};

// The current values of those runtime parameters.
//
// NOTE: This function is defined in `global/RuntimeParameters.cpp`, because the
// headers of the external sorters must not include
// `global/RuntimeParameters.h`. They are (indirectly) included by many
// low-level headers, and that include would define the global
// `RuntimeParameters` object in every translation unit that includes them,
// which then would have to be linked against `global`.
ExternalSorterSettings externalSorterSettings();

}  // namespace ad_utility::compressedExternalIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_EXTERNALSORTERSETTINGS_H
