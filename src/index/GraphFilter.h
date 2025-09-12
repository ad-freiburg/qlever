//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_GRAPHFILTER_H
#define QLEVER_SRC_INDEX_GRAPHFILTER_H

#include <absl/strings/str_join.h>

#include <variant>
#include <vector>

#include "global/ValueId.h"
#include "parser/TripleComponent.h"
#include "util/HashSet.h"
#include "util/OverloadCallOperator.h"

namespace qlever::index {

template <typename T>
class GraphFilter {
 public:
  // Marker type for the "ALL" case.
  struct AllTag {
    bool operator==(const AllTag&) const = default;
  };

  // ALL, WHITELIST, BLACKLIST
  using FilterType = std::variant<AllTag, ad_utility::HashSet<T>, T>;

 private:
  explicit GraphFilter(FilterType filterType);

  FilterType filter_;

 public:
  GraphFilter(const GraphFilter&) = default;
  GraphFilter(GraphFilter&&) = default;
  GraphFilter& operator=(const GraphFilter&) = default;
  GraphFilter& operator=(GraphFilter&&) = default;

  static GraphFilter All();
  static GraphFilter Whitelist(ad_utility::HashSet<T> whitelist);
  static GraphFilter Blacklist(T value);

  template <typename Func>
  GraphFilter<decltype(std::declval<Func>()(std::declval<T>()))> transform(
      const Func& func) const {
    using TransformedT = decltype(func(std::declval<T>()));
    using ResultT = GraphFilter<TransformedT>;
    return std::visit(ad_utility::OverloadCallOperator{
                          [](const AllTag&) { return ResultT::All(); },
                          [&func](const ad_utility::HashSet<T>& whitelist) {
                            ad_utility::HashSet<TransformedT> result;
                            for (const T& value : whitelist) {
                              result.emplace(func(value));
                            }
                            return ResultT::Whitelist(std::move(result));
                          },
                          [&func](const T& blacklist) {
                            return ResultT::Blacklist(func(blacklist));
                          }},
                      filter_);
  }

  bool isGraphAllowed(T graph) const;

  bool isWildcard() const;

  bool operator==(const GraphFilter&) const = default;

  template <typename Formatter>
  void format(std::ostream& os, const Formatter& formatter) const {
    os << "GRAPHS: ";
    std::visit(ad_utility::OverloadCallOperator{
                   [&os](const AllTag&) { os << "ALL"; },
                   [&os, &formatter](const ad_utility::HashSet<T>& whitelist) {
                     // The graphs are stored as a hash set, but we need a
                     // deterministic order.
                     std::vector<std::string> graphIdVec;
                     ql::ranges::transform(
                         whitelist, std::back_inserter(graphIdVec), formatter);
                     ql::ranges::sort(graphIdVec);
                     os << "Whitelist " << absl::StrJoin(graphIdVec, " ")
                        << std::endl;
                   },
                   [&os, &formatter](const T& blacklist) {
                     os << "Blacklist " << std::invoke(formatter, blacklist)
                        << std::endl;
                   }},
               filter_);
  }
};
}  // namespace qlever::index

#endif  // QLEVER_SRC_INDEX_GRAPHFILTER_H
