//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_UTIL_URIPARSERURI_H
#define QLEVER_UTIL_URIPARSERURI_H

#include <absl/cleanup/cleanup.h>
#include <uriparser/Uri.h>

#include <string_view>

namespace qlever::util {

// Wrapper class for the `UriUriA` struct from uriparser. This is needed to
// ensure that the memory allocated for the members of the struct is properly
// freed when the `UriParserUri` object goes out of scope. The destructor of
// `UriParserUri` calls `uriFreeUriMembersA` to free the memory allocated for
// the members of the `UriUriA` struct. This way, we can avoid memory leaks when
// using the `UriParserUri` class to parse URIs with uriparser.
class UriParserUri {
  UriUriA uri_;
  // Store if `uri_` object is initialized and can needs to be cleaned up.
  bool isValid_ = true;

  // Compare if two `UriTextRangeA` objects are equal.
  static bool rangeEqual(const UriTextRangeA& a, const UriTextRangeA& b) {
    return toStringView(a) == toStringView(b);
  }

  // Compare if two `UriPathSegmentA` objects are equal.
  static bool pathEqual(const UriPathSegmentA* a, const UriPathSegmentA* b) {
    while (a && b) {
      if (!rangeEqual(a->text, b->text)) {
        return false;
      }

      a = a->next;
      b = b->next;
    }
    return a == nullptr && b == nullptr;
  }

  explicit UriParserUri(const UriUriA& uri) : uri_{uri} {
    uriMakeOwnerA(&uri_);
  }

 public:
  explicit UriParserUri(std::string_view uriString) {
    auto result = uriParseSingleUriExA(
        &uri_, uriString.data(), uriString.data() + uriString.size(), nullptr);
    AD_CONTRACT_CHECK(result == URI_SUCCESS,
                      "Failed to parse URI: ", uriString);
    uriMakeOwnerA(&uri_);
  }

  // Give const access to the underlying `UriUriA` object.
  const UriUriA& get() const { return uri_; }

  UriParserUri(const UriParserUri& other) noexcept : isValid_{other.isValid_} {
    if (other.isValid_) {
      uriCopyUriA(&uri_, &other.uri_);
    }
  }

  // Resolve the (relative) `uriString` to this URL and return the result.
  UriParserUri resolveUri(std::string_view uriString) const {
    UriUriA relativeIri;
    auto parseResult =
        uriParseSingleUriExA(&relativeIri, uriString.data(),
                             uriString.data() + uriString.size(), nullptr);
    absl::Cleanup cleanupRelativeIri{
        [&relativeIri]() { uriFreeUriMembersA(&relativeIri); }};
    AD_CONTRACT_CHECK(parseResult == URI_SUCCESS,
                      "The passed relative URL is invalid: ", uriString);
    UriUriA resolvedIri;
    auto resolveResult = uriAddBaseUriExA(&resolvedIri, &relativeIri, &uri_,
                                          URI_RESOLVE_STRICTLY);
    absl::Cleanup cleanupResolvedIri{
        [&resolvedIri]() { uriFreeUriMembersA(&resolvedIri); }};
    AD_CONTRACT_CHECK(
        resolveResult == URI_SUCCESS,
        "Could not resolve relative URI. Is the base URI absolute?");
    std::move(cleanupResolvedIri).Cancel();
    return UriParserUri{std::move(resolvedIri)};
  }

  UriParserUri& operator=(const UriParserUri& other) noexcept {
    if (this != &other) {
      if (isValid_) {
        uriFreeUriMembersA(&uri_);
      }
      if (other.isValid_) {
        uriCopyUriA(&uri_, &other.uri_);
      }
      isValid_ = other.isValid_;
    }
    return *this;
  }

  UriParserUri(UriParserUri&& other) noexcept
      : uri_{other.uri_}, isValid_{other.isValid_} {
    other.isValid_ = false;
  }

  UriParserUri& operator=(UriParserUri&& other) noexcept {
    if (this != &other) {
      if (isValid_) {
        uriFreeUriMembersA(&uri_);
      }
      uri_ = other.uri_;
      isValid_ = other.isValid_;
      other.isValid_ = false;
    }
    return *this;
  }

  // Compare `this` and `other` for equality.
  bool operator==(const UriParserUri& other) const {
    const auto& a = this->uri_;
    const auto& b = other.uri_;
    return rangeEqual(a.scheme, b.scheme) &&
           rangeEqual(a.userInfo, b.userInfo) &&
           rangeEqual(a.hostText, b.hostText) &&
           rangeEqual(a.portText, b.portText) &&
           pathEqual(a.pathHead, b.pathHead) && rangeEqual(a.query, b.query) &&
           rangeEqual(a.fragment, b.fragment) &&
           a.absolutePath == b.absolutePath;
  }
  // Unlike C++20, C++17 doesn't rewrite `a != b` as `!(a == b)`, so this has
  // to be provided explicitly.
  bool operator!=(const UriParserUri& other) const { return !(*this == other); }

  // Convert a `UriTextRangeA` to a regular `std::string_view`.
  static std::string_view toStringView(const UriTextRangeA& range) {
    return std::string_view(range.first, range.afterLast - range.first);
  }

  ~UriParserUri() {
    if (isValid_) {
      uriFreeUriMembersA(&uri_);
    }
  }
};
}  // namespace qlever::util

#endif  // QLEVER_UTIL_URIPARSERURI_H
