//
// Created by johannes on 03.06.21.
//

#ifndef QLEVER_GEOMETRICTYPES_H
#define QLEVER_GEOMETRICTYPES_H

#include <re2/re2.h>

#include <optional>
#include <string>

#include "../util/Log.h"

namespace ad_geo {
struct Point {
  float x, y;
  friend auto operator<=>(const Point&, const Point&) = default;

  template <typename H>
  friend H AbslHashValue(H h, const Point& p) {
    return H::combine(std::move(h), p.x, p.y);
  }
};

inline std::string to_string(const Point& p) {
  return std::to_string(p.x) + " " + std::to_string(p.y);
}


struct Rectangle {
  Point lowerLeft, topRight;
  friend auto operator<=>(const Rectangle&, const Rectangle&) = default;
  template <typename H>
  friend H AbslHashValue(H h, const Rectangle& r) {
    return H::combine(std::move(h), r.lowerLeft, r.topRight);
  }
};

inline std::string to_string(const Rectangle& r) {
  using std::to_string;;
  return "LINESTRING(" + to_string(r.lowerLeft) + ", " + to_string(r.topRight) + ")";
}

struct Polygon {
  std::vector<Point> points;
  std::optional<Rectangle> toRectangle() const {
    if (points.size() != 5 || points[0] != points[4]) {
      return std::nullopt;
    }
    auto checkOrthogonal = [](const Point& a, const Point& b) {
      return (a.x == b.x) != (a.y == b.y);
    };
    for (size_t i = 1; i < points.size(); ++i) {
      if (!checkOrthogonal(points[i-1], points[i])) {
        return std::nullopt;
      }
    }
    Rectangle result;
    auto compX = [](const auto& a, const auto&b) {return a.x < b.x;};
    auto compY = [](const auto& a, const auto&b) {return a.y < b.y;};
    result.lowerLeft.x = std::min_element(points.begin(), points.end(), compX)->x;
    result.lowerLeft.y = std::min_element(points.begin(), points.end(), compY)->y;
    result.topRight.x = std::max_element(points.begin(), points.end(), compX)->x;
    result.topRight.y = std::max_element(points.begin(), points.end(), compY)->y;
    return result;
  }
};
inline std::optional<Polygon> parse5Polygon(const std::string& input) {
  std::string number = "([0-9]+(\\.[0-9]+)?)";
  std::string twoNumbers = "\\s*" + number + "\\s+" + number + "\\s*";
  std::string twoNumbersC = ",\\s*" + number + "\\s+" + number + "\\s*";
  std::string fourPoints = twoNumbersC + twoNumbersC + twoNumbersC + twoNumbersC;
  std::string regexString = "\\w*POLYGON\\w*\\(\\w*" + number + "(\\w+" + number + "){4}\\w*\\)\\w*";
  regexString = "\\s*POLYGON\\s*\\(" + twoNumbers + fourPoints + "\\)" ;
  re2::RE2 r{regexString};
  float a, b, c, d, e, f, g, h, i, j;
  bool x = re2::RE2::FullMatch(input, r, &a, nullptr, &b, nullptr, &c, nullptr, &d, nullptr, &e, nullptr, &f, nullptr, &g, nullptr, &h, nullptr, &i, nullptr, &j);
  if (!x) {
    return std::nullopt;
  }
  return Polygon{{{a, b}, {c, d}, {e, f}, {g, h}, {i, j}}};
}

inline std::optional<Rectangle> parseAxisRectancle(const std::string& input) {
  auto polygon = parse5Polygon(input);
  if (!polygon) {
    return std::nullopt;
  }
  return polygon->toRectangle();
}
}


#endif  // QLEVER_GEOMETRICTYPES_H
