// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <chrono>
#include <semaphore>
#include <thread>

#include "../src/util/AsyncStream.h"

using ad_utility::streams::AsyncStream;
using ad_utility::streams::StringSupplier;

std::unique_ptr<StringSupplier> generateNChars(size_t n) {
  class FiniteStream : public StringSupplier {
    size_t _counter = 0;
    size_t _n;

   public:
    explicit FiniteStream(size_t n) : _n{n} {}

    [[nodiscard]] bool hasNext() const override { return _counter < _n; }
    [[nodiscard]] constexpr std::string_view next() override {
      _counter++;
      return "A";
    }
  };
  return std::make_unique<FiniteStream>(n);
}

std::unique_ptr<StringSupplier> waitingStream(
    std::counting_semaphore<2>& semaphore,
    std::vector<std::string_view>& views) {
  class WaitingStream : public StringSupplier {
    std::counting_semaphore<2>& _semaphore;
    std::vector<std::string_view>& _views;
    std::vector<std::string_view>::reverse_iterator _current{};

   public:
    explicit WaitingStream(std::counting_semaphore<2>& semaphore,
                           std::vector<std::string_view>& views)
        : _semaphore{semaphore}, _views{views}, _current{views.rend()} {}
    [[nodiscard]] bool hasNext() const override {
      return _current != _views.rbegin();
    }
    [[nodiscard]] std::string_view next() override {
      _semaphore.acquire();
      _current--;
      return *_current;
    }
  };
  return std::make_unique<WaitingStream>(semaphore, views);
}

/*TEST(AsyncStream, EnsureMaximumBufferLimitWorks) {
  AsyncStream stream{generateNChars(ad_utility::streams::BUFFER_LIMIT + 1)};

  ASSERT_TRUE(stream.hasNext());

  const auto view = stream.next();

  ASSERT_LE(view.size(), ad_utility::streams::BUFFER_LIMIT);
  ASSERT_GT(view.size(), 0);
  for (auto character : view) {
    ASSERT_EQ(character, 'A');
  }
}*/

TEST(AsyncStream, EnsureBuffersAreFilledAndClearedCorrectly) {
  std::vector<std::string_view> strings{"Abc", "Def", "Ghi"};
  std::counting_semaphore<2> semaphore{1};
  auto ptr = waitingStream(semaphore, strings);
  auto streamView = ptr.get();
  AsyncStream stream{std::move(ptr)};

  ASSERT_TRUE(stream.hasNext());

  const auto view = stream.next();

  ASSERT_EQ(view, strings[0]);

  ASSERT_TRUE(stream.hasNext());

  semaphore.release();
  semaphore.release();

  while (streamView->hasNext()) {
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }

  ASSERT_TRUE(stream.hasNext());
  ASSERT_EQ("DefGhi", stream.next());

  ASSERT_FALSE(stream.hasNext());
}
