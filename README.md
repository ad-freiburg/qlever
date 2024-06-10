# QLever

[![Docker build](https://github.com/ad-freiburg/QLever/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/ad-freiburg/QLever/actions/workflows/docker-publish.yml)
[![Native build](https://github.com/ad-freiburg/qlever/actions/workflows/native-build.yml/badge.svg)](https://github.com/ad-freiburg/qlever/actions/workflows/native-build.yml)
[![Format check](https://github.com/ad-freiburg/qlever/actions/workflows/format-check.yml/badge.svg)](https://github.com/ad-freiburg/qlever/actions/workflows/format-check.yml)
[![Test coverage](https://codecov.io/github/ad-freiburg/qlever/branch/master/graph/badge.svg?token=OHcEh02rW0)](https://codecov.io/github/ad-freiburg/qlever)

QLever (pronounced "Clever") is a SPARQL engine that can efficiently index and query very large knowledge graphs with over 100 billion triples on a single standard PC or server.
In particular, QLever is fast for queries that involve large intermediate or final results, which are notoriously hard for engines like Blazegraph or Virtuoso.
QLever also supports search in text associated with the knowledge base, as well as SPARQL autocompletion.

[Here are demos of QLever](http://qlever.cs.uni-freiburg.de) on a variety of large knowledge graphs, including the complete Wikidata, Wikimedia Commons, OpenStreetMap, UniProt, PubChem, and DBLP.
Those demos also feature QLever's context-sensitive autocompletion, which makes SPARQL query construction so much easier. The knowledge graphs are updated regularly. Click on "Index Information" for a short description (with dates) and basic statistics.

If you use QLever in your research work, please cite one of the following publications:
our [CIKM'17 paper](https://ad-publications.informatik.uni-freiburg.de/CIKM_qlever_BB_2017.pdf) (combination of SPARQL and text search, with extensive evaluation),
our [CIKM'22 paper](https://ad-publications.cs.uni-freiburg.de/CIKM_sparql_autocompletion_BKKKS_2022.pdf) (QLever's autocompletion, with extensive evaluation),
our [2023 book chapter](https://ad-publications.cs.uni-freiburg.de/CHAPTER_knowledge_graphs_BKKK_2023.pdf) (survey of knowledge graphs and basics of QLever, with many example queries).

QLever aims at full SPARQL 1.1 support and is almost there. In particular, a first version of SPARQL 1.1 Federated Query (SERVICE) is implemented since [PR #793](https://github.com/ad-freiburg/qlever/pull/793) and a proof of concept for SPARQL 1.1 Update is implemented since [PR #916](https://github.com/ad-freiburg/qlever/pull/916). If you find a bug in QLever or in one of our demos or if you are missing a feature, please [open an issue](https://github.com/ad-freiburg/qlever/issues).

# Quickstart

Use QLever via the `qlever` script, following the instructions on https://github.com/ad-freiburg/qlever-control .
The script allows you to control all things QLever does, with all the configuration in one place, the so-called `Qleverfile`.
The script comes with a number of example `Qleverfile`s (in particular, one for each of the demos mentioned above),
which makes it very easy to get started and also helps to write your own `Qleverfile` for your own data. If you use
QLever via docker (which is the default setting), the script pulls the most recent docker image automatically and you
don't have to download or compile the code.

If the `qlever` script does not work for you for whatever reason, have a look at the [Dockerfile for Ubuntu 22.04](https://github.com/ad-freiburg/qlever/blob/master/Dockerfile) or the [Dockerfiles for older Ubuntu versions](https://github.com/ad-freiburg/qlever/tree/master/Dockerfiles). The [source code of the qlever script](https://github.com/ad-freiburg/qlever-control/blob/main/qlever) also provides information on how to use QLever (in particular, note the functions `action_start` and `action_index`).

An older (and not quite up-to-date anymore) step-by-step instruction can be found [here](docs/quickstart.md).
QLever's [advanced features are described here](docs/advanced_features.md).
For more in-depth information, see the various other `.md` files in [this folder](docs), some of which are outdated though.
For high-level descriptions how Qlever works and experiences with some concrete datasets, see the [Qlever Wiki](https://github.com/ad-freiburg/qlever/wiki).
