# Wikidata Full Quickstart

The following instructions take you through loading the full Wikidata Knowledge
Base in QLever. Since Wikidata is very large at over 7 billion triples, **we
recommend to run all of the following commands in a `tmux` or `screen` session**.
Nevertheless we will note below when a command will take long to execute.

## Download and build QLever using `docker`

We build QLever using `docker` which takes care of all necessary dependencies.
This should only take a couple of minutes

    git clone --recursive https://github.com/ad-freiburg/QLever.git qlever
    cd qlever
    docker build -t qlever .

## Download Wikidata

If you already downloaded **and decrompressed** Wikidata to uncompressed Turtle
format you can skip this step, otherwise we download and uncompress it.

**Note:** This takes several hours as Wikidata is about 42 GB compressed and the
servers are throttled. Also make sure you have enough space on the filesystem
where you unpacked QLever or see the full
[README](https://github.com/ad-freiburg/QLever#building-the-index) for
instructions on using a different path for the index.

**The index plus unpacked Wikidata will use up to about 2 TB.**

    wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.ttl.bz2
    bunzip2 latest-all.ttl.bz2

## Build a QLever Index

Now we can build a QLever Index from the `latest-all.ttl` Wikidata Turtle file
using the `wikidata_settings.json` file for some useful default settings for
relations that can be safely stored on disk because their actual values are
rarely needed. If you're using a different path for the `latest-all.ttl` file
**you must make sure that the settings file is accessible from within the
container**.

**Note (0):** If you are using `docker` with user namespaces or your user id (`id
-u`) is not 1000 you have to make the `./index` folder writable for QLever
inside the container e.g. by running `chmod -R o+rw ./index`

**Note (1):** This takes about half a day but should be much faster than with most
other Triple Stores.

    docker run -it --rm \
        -v "$(pwd)/:/input" \  # If your latest-all.ttl is
        -v "$(pwd)/index:/index" --entrypoint "bash" qlever
    qlever@xyz:/app$ IndexBuilderMain -a -l -i /index/wikidata-full \
        -n /input/latest-all.ttl \
        -s /input/wikidata_settings.json
    … wait for about half a day …
    qlever@xyz:/app$ exit

## Run QLever

Finally we are ready to launch a QLever instance using the newly build Wikidata
index.

    docker run -it -p 7001:7001 \
        -v "$(pwd)/index:/index" \
        -e INDEX_PREFIX=wikidata-full \
        --name qlever \
        qlever

Then point your browser to [http://localhost:7001/](http://localhost:7001/) and
enter the query.

For example the following query retrieves all mountains above 8000 m

    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX p: <http://www.wikidata.org/prop/>
    PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?subj ?label ?coord ?elev WHERE
    {
      ?subj wdt:P31 wd:Q8502 .
      ?subj p:P2044 ?elev_s .
      ?elev_s psn:P2044 ?elev_v .
      ?elev_v wikibase:quantityAmount ?elev .
      ?subj wdt:P625 ?coord .
      ?subj rdfs:label ?label .
      FILTER (?elev > 8000.0) .
      FILTER langMatches(lang(?label), "en") .
    }
    ORDER BY DESC(?elev)
