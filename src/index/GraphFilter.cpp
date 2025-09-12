//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "index/GraphFilter.h"

#include "util/graph/Graph.h"

namespace qlever::index {

//______________________________________________________________________________
template <typename T>
GraphFilter<T>::GraphFilter(FilterType filterType)
    : filter_{std::move(filterType)} {}

//______________________________________________________________________________
template <typename T>
GraphFilter<T> GraphFilter<T>::All() {
  return GraphFilter{AllTag{}};
}

//______________________________________________________________________________
template <typename T>
GraphFilter<T> GraphFilter<T>::Whitelist(ad_utility::HashSet<T> whitelist) {
  return GraphFilter{std::move(whitelist)};
}

//______________________________________________________________________________
template <typename T>
GraphFilter<T> GraphFilter<T>::Blacklist(T value) {
  return GraphFilter{std::move(value)};
}

//______________________________________________________________________________
template <typename T>
bool GraphFilter<T>::isGraphAllowed(T graph) const {
  return std::visit(
      ad_utility::OverloadCallOperator{
          [](const AllTag&) { return true; },
          [graph](const ad_utility::HashSet<T>& whitelist) {
            return whitelist.contains(graph);
          },
          [graph](const T& blacklist) { return graph != blacklist; }},
      filter_);
}

//______________________________________________________________________________
template <typename T>
bool GraphFilter<T>::isWildcard() const {
  return std::holds_alternative<AllTag>(filter_);
}

//______________________________________________________________________________
template class GraphFilter<TripleComponent>;
template class GraphFilter<Id>;
}  // namespace qlever::index
