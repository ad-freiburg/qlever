//
// Created by kalmbacj on 9/2/25.
//

// TODO<joka921> header guard.
#ifndef KEYWORDS_H
#define KEYWORDS_H

// TODO<joka921> Comments.
#ifdef QLEVER_CPP_17
#define QL_CONSTEVAL constexpr
#define QL_EXPLICIT(...)

#else
#define QL_CONSTEVAL consteval
#define QL_EXPLICIT(...) explicit(__VA_ARGS__)
#endif

#endif  // KEYWORDS_H
