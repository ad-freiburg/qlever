//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_UTIL_URIPARSERURI_H
#define QLEVER_UTIL_URIPARSERURI_H

#include <uriparser/Uri.h>

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
  static int rangeEqual(const UriTextRangeA* a, const UriTextRangeA* b) {
    size_t lenA = a->afterLast - a->first;
    size_t lenB = b->afterLast - b->first;

    if (lenA != lenB) return 0;

    if (lenA == 0) return 1;

    return memcmp(a->first, b->first, lenA) == 0;
  }

  // Compare if two `UriPathSegmentA` objects are equal.
  static int pathEqual(const UriPathSegmentA* a, const UriPathSegmentA* b) {
    while (a && b) {
      if (!rangeEqual(&a->text, &b->text)) return 0;

      a = a->next;
      b = b->next;
    }
    return a == nullptr && b == nullptr;
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

  bool operator==(const UriParserUri& other) const {
    const auto& a = this->uri_;
    const auto& b = other.uri_;
    return rangeEqual(&a.scheme, &b.scheme) &&
           rangeEqual(&a.userInfo, &b.userInfo) &&
           rangeEqual(&a.hostText, &b.hostText) &&
           rangeEqual(&a.portText, &b.portText) &&
           pathEqual(a.pathHead, b.pathHead) &&
           rangeEqual(&a.query, &b.query) &&
           rangeEqual(&a.fragment, &b.fragment) &&
           a.absolutePath == b.absolutePath;
  }

  ~UriParserUri() {
    if (isValid_) {
      uriFreeUriMembersA(&uri_);
    }
  }
};

#endif  // QLEVER_UTIL_URIPARSERURI_H
