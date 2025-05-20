// Copyright 2025 The QLever Authors, in particular:
//
// 2022 - 2023 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2025        Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_HTTPCLIENTTESTHELPERS_H
#define QLEVER_HTTPCLIENTTESTHELPERS_H

#include <gtest/gtest.h>

#include <exception>
#include <string>

#include "./GTestHelpers.h"
#include "util/http/HttpClient.h"
#include "util/http/HttpUtils.h"

struct RequestMatchers {
  std::optional<testing::Matcher<std::string>> url_;
  std::optional<testing::Matcher<const boost::beast::http::verb&>> method_;
  std::optional<testing::Matcher<std::string_view>> postData_;
  std::optional<testing::Matcher<std::string_view>> contentType_;
  std::optional<testing::Matcher<std::string_view>> accept_;
};

// Factory for generating mocks of the `sendHttpOrHttpsRequest` function. Can
// also test that the request is expected via `matchers`.
static auto constexpr getResultFunctionFactory =
    [](std::string predefinedResult, std::string contentType = "",
       boost::beast::http::status status = boost::beast::http::status::ok,
       RequestMatchers matchers_ = {},
       std::exception_ptr mockException = nullptr,
       ad_utility::source_location loc =
           ad_utility::source_location::current()) -> sendRequestType {
  return
      [=](const ad_utility::httpUtils::Url& url,
          ad_utility::SharedCancellationHandle,
          const boost::beast::http::verb& method, std::string_view postData,
          std::string_view contentTypeHeader, std::string_view acceptHeader) {
        auto g = generateLocationTrace(loc);
        // Check that the request parameters are as expected, e.g. that a
        // request is sent to the correct url with the expected body.
        if (matchers_.url_) {
          EXPECT_THAT(url.asString(), matchers_.url_.value());
        }
        if (matchers_.method_) {
          EXPECT_THAT(method, matchers_.method_.value());
        }
        if (matchers_.postData_) {
          EXPECT_THAT(postData, matchers_.postData_.value());
        }
        if (matchers_.contentType_) {
          EXPECT_THAT(contentTypeHeader, matchers_.contentType_.value());
        }
        if (matchers_.accept_) {
          EXPECT_THAT(acceptHeader, matchers_.accept_.value());
        }

        if (mockException) {
          std::rethrow_exception(mockException);
        }

        auto body =
            [](std::string result) -> cppcoro::generator<ql::span<std::byte>> {
          // Randomly slice the string to make tests more robust.
          std::mt19937 rng{std::random_device{}()};

          const std::string resultStr = result;
          std::uniform_int_distribution<size_t> distribution{
              0, resultStr.length() / 2};

          for (size_t start = 0; start < resultStr.length();) {
            size_t size = distribution(rng);
            std::string resultCopy{resultStr.substr(start, size)};
            co_yield ql::as_writable_bytes(ql::span{resultCopy});
            start += size;
          }
        };
        return HttpOrHttpsResponse{.status_ = status,
                                   .contentType_ = contentType,
                                   .body_ = body(predefinedResult)};
      };
};

#endif  // QLEVER_HTTPCLIENTTESTHELPERS_H
