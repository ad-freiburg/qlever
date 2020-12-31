//
// Created by johannes on 19.12.20.
//
#include <iostream>
#include <vector>

#ifdef _PARALLEL_SORT
#include "./CancelableParallelSort.h"
#include "./CancelableSequentialSort.h"
#endif

int main(void) {
#ifdef _PARALLEL_SORT
  ad_utility::TimeoutChecker c{ad_utility::TimeoutTimer::secLimited(50)};

  ad_utility::sequential::TimeoutedAlgorithms algo{&c};
  std::vector<int> x;
  for (size_t i = 500000; i > 0; --i) {
    x.push_back(i);
  }
  std::cout << "finished writing, start sorting" << std::endl;

  c.wlock()->start();
  // algo.sort(x.begin(), x.end());

  ad_utility::cancelableParallelSort::ParallelSorter p{&c};
  std::cout << "start parallel sorting" << std::endl;
  try {
    p.sort(x.begin(), x.end(), std::less<>{}, 4);
  } catch (const std::runtime_error& e) {
    std::cout << e.what() << std::endl;
  }
  std::cout << "finished sorting" << std::endl;

  return x[0];
#else
  std::cerr << "You need to compile this example with support for parallel "
               "sorting to be useful\n";
#endif
}
