//
// Created by kalmbacj on 12/10/24.
//

#include <vector>

#include "engine/idTable/IdTable.h"
#include "util/Generator.h"
#include "util/Views.h"

cppcoro::generator<std::vector<int>> inner() { return {}; }

auto joinOwning() { return ql::views::join(ad_utility::OwningView{inner()}); }

/*
auto joinOwning() {
  return
ql::views::join(ad_utility::OwningView{std::vector<std::vector<int>>{}});
}
*/

auto vec() {
  std::vector<decltype(joinOwning())> vec;
  vec.push_back(joinOwning());
  return vec;
}

auto joinOuter() {
  // return ad_utility::OwningView{vec()};
  // return ql::views::join(ad_utility::OwningView<decltype(vec()),
  // true>{vec()});
  return ql::views::join(ad_utility::OwningViewNoConst{vec()});
}

int main() {
  auto view = joinOuter();
  [[maybe_unused]] auto it = view.begin();
}
