# QLever

[![Docker build](https://github.com/ad-freiburg/QLever/actions/workflows/docker-build.yml/badge.svg)](https://github.com/ad-freiburg/QLever/actions/workflows/docker-build.yml)
[![Native build](https://github.com/ad-freiburg/qlever/actions/workflows/native-build.yml/badge.svg)](https://github.com/ad-freiburg/qlever/actions/workflows/native-build.yml)
[![Format check](https://github.com/ad-freiburg/qlever/actions/workflows/format-check.yml/badge.svg)](https://github.com/ad-freiburg/qlever/actions/workflows/format-check.yml)
[![Test coverage](https://codecov.io/github/ad-freiburg/qlever/branch/master/graph/badge.svg?token=OHcEh02rW0)](https://codecov.io/github/ad-freiburg/qlever)

QLever (pronounced "Clever") is a SPARQL engine that can efficiently index and query very large knowledge graphs with up to 100 billion triples on a single standard PC or server.
In particular, QLever is fast for queries that involve large intermediate or final results, which are notoriously hard for engines like Blazegraph or Virtuoso.
QLever also supports search in text associated with the knowledge base, as well as SPARQL autocompletion.

[Here are demos of QLever](http://qlever.cs.uni-freiburg.de) on a variety of large knowledge graphs, including the complete Wikidata, OpenStreetMap, PubChem, and DBLP.
Those demos also feature QLever's context-sensitiv autocompletion, which makes SPARQL query construction so much easier. The datasets for Wikdiata and DBLP are updated automatically and hence always the latest versions (click on "Index Information").

QLever was first described and evaluated in this [CIKM'17
paper](http://ad-publications.informatik.uni-freiburg.de/CIKM_qlever_BB_2017.pdf).
QLever has developed a lot since then.
Qlever's autocompletion functionality and some other new features are described and evaluated in [this paper](https://ad-publications.cs.uni-freiburg.de/ARXIV_sparql_autocompletion_BKKKS_2021.pdf).
If you use QLever in your work, please cite those papers.
QLever supports standard SPARQL 1.1 constructs like:
LIMIT, OFFSET, ORDER BY, GROUP BY, HAVING, COUNT, DISTINCT, SAMPLE, GROUP_CONCAT, FILTER, REGEX, LANG, OPTIONAL, UNION, MINUS, VALUES, BIND.
Predicate paths and subqueries are also supported.
The SERVICE keyword is not yet supported, but we are working on it.
We aim at full SPARQL 1.1 support and we are almost there (except for SPARQL Update operations, which are a longer-term project).

# Quickstart

Use QLever via the `qlever` script, following the instructions on https://github.com/ad-freiburg/qlever-control .
The script allows you to control all things QLever does, with all the configuration in one place, the so-called `Qleverfile`.
The script comes with a number of example `Qleverfile`s (in particular, one for each of the demos mentioned above),
which makes it very easy to get started and also helps to write your own `Qleverfile` for your own data. If you use
QLever via docker (which is the default setting), the script pulls the most recent docker image automatically and you
don't have to download or compile the code.

# Advanced feature, in-depth information, and older documentation

An older (and not quite up-to-date anymore) step-by-step instruction can be found [here](docs/quickstart.md).
QLever's [advanced features are described here](docs/advanced_features.md).
For more in-depth information, see the various other `.md` files in [this folder](docs), some of which are outdated though.
For high-level descriptions how Qlever works and experiences with some concrete datasets, see the [Qlever Wiki](https://github.com/ad-freiburg/qlever/wiki).
