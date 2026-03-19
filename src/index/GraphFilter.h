//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_GRAPHFILTER_H
#define QLEVER_SRC_INDEX_GRAPHFILTER_H

#include <absl/functional/function_ref.h>
#include <absl/strings/str_join.h>

#include <variant>

#include "backports/three_way_comparison.h"
#include "global/ValueId.h"
#include "parser/TripleComponent.h"
#include "util/HashSet.h"
#include "util/OverloadCallOperator.h"
#include "util/TypeTraits.h"

namespace qlever::index {

// Class that represents the concept of a graph filter. It can store a whitelist
// of multiple graphs or a blacklist of a single graph or be no-op and provides
// an interface that simply tells you if a specific graph is allowed by this
// filter. The template parameter `T` indicates how a graph is represented.
// Currently, this is either `TripleComponent` or `ValueId`.
template <typename T>
class GraphFilter {
 public:
  // Marker type for the "ALL" case.
  struct AllTag {
    QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(AllTag)
  };

  // ALL, WHITELIST, BLACKLIST
  using FilterType = std::variant<AllTag, ad_utility::HashSet<T>, T>;

 private:
  explicit GraphFilter(FilterType filterType);

  // Underlying storage of filtered values.
  FilterType filter_;

 public:
  GraphFilter(const GraphFilter&) = default;
  GraphFilter(GraphFilter&&) noexcept = default;
  GraphFilter& operator=(const GraphFilter&) = default;
  GraphFilter& operator=(GraphFilter&&) noexcept = default;

  // Keep all graphs.
  static GraphFilter All();
  // Only keep graphs in `whitelist`.
  static GraphFilter Whitelist(ad_utility::HashSet<T> whitelist);
  // Keep all graphs that are not `value`.
  static GraphFilter Blacklist(T value);

  // Transforms this `GraphFilter` into another one with a different template
  // argument, by using `func` to transform the underlying values from `T` to a
  // new type.
  CPP_template(typename Func)(requires ql::concepts::invocable<Func, const T&>)
      GraphFilter<std::invoke_result_t<Func, T>> transform(
          const Func& func) const {
    using TransformedT = std::invoke_result_t<Func, T>;
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

  // Return true iff the graph `graph´ is allowed by this filer.
  bool isGraphAllowed(T graph) const;

  // Return true iff all graphs are always allowed.
  bool areAllGraphsAllowed() const;

  // Make sure this filter is comparable.
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(GraphFilter, filter_)

  // Describe this `GraphFilter` and write it to the output stream using
  // `formatter` to format the individual graph values `T`.
  void format(std::ostream& os,
              absl::FunctionRef<std::string(const T&)> formatter) const;
};
}  // namespace qlever::index

#endif  // QLEVER_SRC_INDEX_GRAPHFILTER_H
