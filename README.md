# QLever


[![Build Status via Docker](https://github.com/ad-freiburg/QLever/actions/workflows/docker.yml/badge.svg)](https://github.com/ad-freiburg/QLever/actions/workflows/docker.yml)
[![Build Status via G++10/Clang++11](https://github.com/ad-freiburg/QLever/actions/workflows/cmake.yml/badge.svg)](https://github.com/ad-freiburg/QLever/actions/workflows/cmake.yml)



QLever (pronounced "Clever") is a SPARQL engine that can efficiently index and query very large knowledge graphs with up to 100 billion triples on a single standard PC or server.
In particular, QLever is fast for queries that involve large intermediate or final results, which are notoriously hard for engines like Blazegraph or Virtuoso.
QLever also supports search in text associated with the knowledge base, as well as SPARQL autocompletion.
[Here are demos of QLever](http://qlever.cs.uni-freiburg.de) on a variety of large knowledge graphs, including the complete Wikidata and OpenStreetMap.
Those demos also feature QLever's context-sensitiv autocompletion, which makes SPARQL query construction so much easier.

QLever was first described and evaluated in this [CIKM'17
paper](http://ad-publications.informatik.uni-freiburg.de/CIKM_qlever_BB_2017.pdf).
QLever has developed a lot since then.
Qlever's autocompletion functionality and some other new features are described and evaluated in [this paper](https://ad-publications.cs.uni-freiburg.de/ARXIV_sparql_autocompletion_BKKKS_2021.pdf).
If you use QLever in your work, please cite those papers.
QLever supports standard SPARQL 1.1 constructs like:
LIMIT, OFFSET, ORDER BY, GROUP BY, HAVING, COUNT, DISTINCT, SAMPLE, GROUP_CONCAT, FILTER, REGEX, LANG, OPTIONAL, UNION, MINUS, VALUES, BIND.
Predicate paths and subqueries are also supported.
The SERVICE keyword is not yet supported.
We aim at full SPARQL 1.1 support and we are almost there (except for SPARQL Update operations, which are a longer-term project).

# Quickstart

For easy step-by-step instructions on how to build an index using QLever and
then start a SPARQL endpoint using that index, see our [Quickstart Guide](docs/quickstart.md).
This will take you through a simple example dataset ([120 Years of Olympics](https://github.com/wallscope/olympics-rdf), with 1.8M triples)
as well as a very large dataset ([the complete Wikidata](https://www.wikidata.org), with 16 billion triples as of 30.09.2021).

# Advanced feature and more in-depth information

QLever's [advanced features are described here](docs/advanced_features.md).

For more in-depth information, see the various other `.md` files in [this folder](docs).
