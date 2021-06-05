//
// Created by johannes on 03.06.21.
//

#ifndef QLEVER_GEOMETRICTYPES_H
#define QLEVER_GEOMETRICTYPES_H

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
  return "POINT(" + std::to_string(p.x) + " " + std::to_string(p.y) + ")";
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
  return "RECTANGLE(" + to_string(r.lowerLeft) + " " + to_string(r.topRight) + ")";
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
}


#endif  // QLEVER_GEOMETRICTYPES_H
