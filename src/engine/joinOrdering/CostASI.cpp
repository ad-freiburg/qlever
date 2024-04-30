// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "CostASI.h"

namespace JoinOrdering::ASI {

template <typename N>
requires RelationAble<N> auto rank(QueryGraph<N>& g, N n) -> float {
  // TODO: unpack hist here?
  std::vector<N> seq{n};

  // assert rank [0, 1]
  return (T(g, seq) - 1) / C(g, seq);
}

// TODO: std::span
template <typename N>
requires RelationAble<N>
auto T(QueryGraph<N>& g, const std::vector<N>& seq) -> float {
  return std::transform_reduce(
      seq.begin(), seq.end(), 1.0f, std::multiplies{}, [&](const N& n) {
        return g.selectivity.at(n) * static_cast<float>(n.getCardinality());
      });
}

template <typename N>
requires RelationAble<N>
auto C(QueryGraph<N>& g, const std::vector<N>& seq) -> float {
  std::vector<N> v{};

  for (auto const& x : seq)
    //    if (hist.contains(x) && hist.at(x).empty())
    if (g.hist[x].empty())
      v.push_back(x);
    else
      for (auto const& h : g.hist.at(x)) v.push_back(h);
  // return 0 if Ri is root 113/637
  //    if (v.size() == 1 && v.front() == root) return 0;

  if (v.empty()) return 0;
  if (v.size() == 1)
    return g.selectivity.at(v.front()) *
           static_cast<float>(v.front().getCardinality());  // T(v)

  //    auto s1 = seq | std::views::take(1);
  //    auto s2 = seq | std::views::drop(1);

  auto s1 = std::vector<N>{v.front()};
  auto s2 = std::vector<N>(v.begin() + 1, v.end());

  // std::span(v.begin()+1, v.end())
  return C(g, s1) + T(g, s1) * C(g, s2);
}

template <typename N>
requires RelationAble<N>
auto C(QueryGraph<N>& g, const std::set<N>& seq) -> float {
  std::vector<N> t(seq.begin(), seq.end());
  return C(g, t);
}
}  // namespace JoinOrdering::ASI
