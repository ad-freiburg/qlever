// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "util/GeometryInfo.h"

#include <spatialjoin/WKTParse.h>
#include <util/geo/Geo.h>

#include "parser/Literal.h"
#include "parser/NormalizedString.h"

GeometryInfo GeometryInfo::fromWktLiteral(const std::string_view& wkt) {
  auto lit = ad_utility::triple_component::Literal::fromStringRepresentation(
      std::string(wkt));
  auto wktLiteral = asStringViewUnsafe(lit.getContent());

  // TODO<ullingerc> Replace this dummy with computation
  // TODO<ullingerc> How can we retrieve libspatialjoin offsets?
  return GeometryInfo{
      static_cast<uint8_t>(::util::geo::getWKTType(std::string(wktLiteral)))};
  // util::geo::getWKTType()
}
