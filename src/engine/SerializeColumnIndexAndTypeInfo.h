//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/VariableToColumnMap.h"
#include "util/Serializer/Serializer.h"

// Serialization for ColumnIndexAndTypeInfo
AD_SERIALIZE_FUNCTION(ColumnIndexAndTypeInfo) {
  if constexpr (ad_utility::serialization::WriteSerializer<S>) {
    serializer | arg.columnIndex_;
    uint8_t status = static_cast<uint8_t>(arg.mightContainUndef_);
    serializer | status;
  } else {
    ColumnIndex colIndex;
    serializer | colIndex;
    uint8_t status;
    serializer | status;
    arg = ColumnIndexAndTypeInfo{
        colIndex, static_cast<ColumnIndexAndTypeInfo::UndefStatus>(status)};
  }
}
