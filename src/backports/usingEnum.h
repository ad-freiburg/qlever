//
// Created by kalmbacj on 9/2/25.
//

#ifndef USINGENUM_H
#define USINGENUM_H

// TODO<BMW>
// 1. Document, including the manual definition with explicit values.
// 2. Document the current limitations (up to 10 entries, can be extended
// manually below).
// 3. apply this for all enums that occur with `using enum`.
// 4. This doesn't work if the enum is declared/defined inside a class. In this
// case it should work
//    to define the enum before the class in a suitable namespace.

// Manual implementation for enums with explicit values
#define QL_DEFINE_ENUM_MANUAL(EnumName, ...) \
  enum class EnumName { __VA_ARGS__ };       \
  namespace ql_using_enum_namespace_##EnumName {

#define QL_DEFINE_TYPED_ENUM_MANUAL(EnumName, EnumType, ...) \
  enum class EnumName : EnumType { __VA_ARGS__ };             \
  namespace ql_using_enum_namespace_##EnumName {

// Add this after QL_DEFINE_ENUM_MANUAL for each enum value
#define QL_ENUM_ALIAS(EnumName, name) \
  static constexpr EnumName name = EnumName::name;

// Close the namespace
#define QL_DEFINE_ENUM_END() }

// Automatic version for simple enum values (identifiers only, no assignments)
#define QL_DEFINE_ENUM(EnumName, ...)             \
  enum class EnumName { __VA_ARGS__ };            \
  namespace ql_using_enum_namespace_##EnumName {  \
    QL_DEFINE_ENUM_ALIASES(EnumName, __VA_ARGS__) \
  }

#define QL_DEFINE_SCOPED_ENUM(ScopeName, EnumName, ...) \
  namespace ql_scoped_namespace_##ScopeName {           \
    QL_DEFINE_ENUM(EnumName, __VA_ARGS__)               \
  }

#define QL_DEFINE_TYPED_ENUM(EnumName, EnumType, ...) \
  enum class EnumName : EnumType { __VA_ARGS__ };     \
  namespace ql_using_enum_namespace_##EnumName {      \
    QL_DEFINE_ENUM_ALIASES(EnumName, __VA_ARGS__)     \
  }

#define QL_DEFINE_SCOPED_TYPED_ENUM(ScopeName, EnumName, EnumType, ...) \
  namespace ql_scoped_namespace_##ScopeName {                           \
    QL_DEFINE_TYPED_ENUM(EnumName, EnumType, __VA_ARGS__)               \
  }

// Alias generation for simple cases
#define QL_DEFINE_ENUM_ALIASES(EnumName, ...) \
  QL_DEFINE_ENUM_DISPATCH(__VA_ARGS__)(EnumName, __VA_ARGS__)

#define QL_DEFINE_ENUM_DISPATCH(...)                                           \
  QL_DEFINE_ENUM_COUNT(__VA_ARGS__, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21,    \
                       20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, \
                       5, 4, 3, 2, 1)

#define QL_DEFINE_ENUM_COUNT(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11,     \
                             _12, _13, _14, _15, _16, _17, _18, _19, _20, _21, \
                             _22, _23, _24, _25, _26, _27, _28, _29, _30, N,   \
                             ...)                                              \
  QL_DEFINE_ENUM_##N

#define QL_DEFINE_ENUM_1(EnumName, a) static constexpr EnumName a = EnumName::a;
#define QL_DEFINE_ENUM_2(EnumName, a, b)     \
  static constexpr EnumName a = EnumName::a; \
  static constexpr EnumName b = EnumName::b;
#define QL_DEFINE_ENUM_3(EnumName, a, b, c)  \
  static constexpr EnumName a = EnumName::a; \
  static constexpr EnumName b = EnumName::b; \
  static constexpr EnumName c = EnumName::c;
#define QL_DEFINE_ENUM_4(EnumName, a, b, c, d) \
  QL_DEFINE_ENUM_2(EnumName, a, b);            \
  QL_DEFINE_ENUM_2(EnumName, c, d);
#define QL_DEFINE_ENUM_5(EnumName, a, b, c, d, e) \
  QL_DEFINE_ENUM_3(EnumName, a, b, c);            \
  QL_DEFINE_ENUM_2(EnumName, d, e);
#define QL_DEFINE_ENUM_6(EnumName, a, b, c, d, e, f) \
  QL_DEFINE_ENUM_3(EnumName, a, b, c);               \
  QL_DEFINE_ENUM_3(EnumName, d, e, f);
#define QL_DEFINE_ENUM_7(EnumName, a, b, c, d, e, f, g) \
  QL_DEFINE_ENUM_4(EnumName, a, b, c, d);               \
  QL_DEFINE_ENUM_3(EnumName, e, f, g);
#define QL_DEFINE_ENUM_8(EnumName, a, b, c, d, e, f, g, h) \
  QL_DEFINE_ENUM_4(EnumName, a, b, c, d);                  \
  QL_DEFINE_ENUM_4(EnumName, e, f, g, h);
#define QL_DEFINE_ENUM_9(EnumName, a, b, c, d, e, f, g, h, i) \
  QL_DEFINE_ENUM_4(EnumName, a, b, c, d);                     \
  QL_DEFINE_ENUM_5(EnumName, e, f, g, h, i);
#define QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j) \
  QL_DEFINE_ENUM_4(EnumName, a, b, c, d);                         \
  QL_DEFINE_ENUM_6(EnumName, e, f, g, h, i, j);
#define QL_DEFINE_ENUM_11(EnumName, a, b, c, d, e, f, g, h, i, j, k) \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);         \
  QL_DEFINE_ENUM_1(EnumName, k);
#define QL_DEFINE_ENUM_12(EnumName, a, b, c, d, e, f, g, h, i, j, k, l) \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);            \
  QL_DEFINE_ENUM_2(EnumName, k, l);
#define QL_DEFINE_ENUM_13(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m) \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);               \
  QL_DEFINE_ENUM_3(EnumName, k, l, m);
#define QL_DEFINE_ENUM_14(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n) \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_4(EnumName, k, l, m, n);
#define QL_DEFINE_ENUM_15(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o)                                                  \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_5(EnumName, k, l, m, n, o);
#define QL_DEFINE_ENUM_16(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p)                                               \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_6(EnumName, k, l, m, n, o, p);
#define QL_DEFINE_ENUM_17(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q)                                            \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_7(EnumName, k, l, m, n, o, p, q);
#define QL_DEFINE_ENUM_18(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r)                                         \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_8(EnumName, k, l, m, n, o, p, q, r);
#define QL_DEFINE_ENUM_19(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r, s)                                      \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_9(EnumName, k, l, m, n, o, p, q, r, s);
#define QL_DEFINE_ENUM_20(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r, s, t)                                   \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_10(EnumName, k, l, m, n, o, p, q, r, s, t);
#define QL_DEFINE_ENUM_21(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r, s, t, u)                                \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_11(EnumName, k, l, m, n, o, p, q, r, s, t, u);
#define QL_DEFINE_ENUM_22(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r, s, t, u, v)                             \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_12(EnumName, k, l, m, n, o, p, q, r, s, t, u, v);
#define QL_DEFINE_ENUM_23(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r, s, t, u, v, w)                          \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_13(EnumName, k, l, m, n, o, p, q, r, s, t, u, v, w);
#define QL_DEFINE_ENUM_24(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r, s, t, u, v, w, x)                       \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_14(EnumName, k, l, m, n, o, p, q, r, s, t, u, v, w, x);
#define QL_DEFINE_ENUM_25(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r, s, t, u, v, w, x, y)                    \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_15(EnumName, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y);
#define QL_DEFINE_ENUM_26(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r, s, t, u, v, w, x, y, z)                 \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_16(EnumName, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z);
#define QL_DEFINE_ENUM_27(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r, s, t, u, v, w, x, y, z, aa)             \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_17(EnumName, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, \
                    aa);
#define QL_DEFINE_ENUM_28(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r, s, t, u, v, w, x, y, z, aa, ab)         \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_18(EnumName, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, \
                    aa, ab);
#define QL_DEFINE_ENUM_29(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r, s, t, u, v, w, x, y, z, aa, ab, ac)     \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_19(EnumName, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, \
                    aa, ab, ac);
#define QL_DEFINE_ENUM_30(EnumName, a, b, c, d, e, f, g, h, i, j, k, l, m, n, \
                          o, p, q, r, s, t, u, v, w, x, y, z, aa, ab, ac, ad) \
  QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j);                  \
  QL_DEFINE_ENUM_20(EnumName, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, \
                    aa, ab, ac, ad);

#define QL_USING_ENUM(EnumName) \
  using namespace ql_using_enum_namespace_##EnumName;

// Replacement for `QL_USING_ENUM` when using an enum for a different namespace.
#define QL_USING_ENUM_NAMESPACE(Namespace, EnumName) \
  using namespace Namespace::ql_using_enum_namespace_##EnumName;

#define QL_USING_SCOPED_ENUM_NAMESPACE(ScopeName, EnumName) \
  using namespace ql_scoped_namespace_##ScopeName::         \
      ql_using_enum_namespace_##EnumName;

#define QL_USING_SCOPED_ENUM(ScopeName, EnumName) \
  using EnumName = ql_scoped_namespace_##ScopeName::EnumName;

#endif  // USINGENUM_H
