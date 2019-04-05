# Wikidata Full Quickstart

The following instructions take you through loading the full Wikidata Knowledge
Base into QLever. Since Wikidata is very large at over 7 billion triples, **we
recommend to run all of the following commands in a `tmux` or `screen` session**.
Furthermore, commands that take a long time will be marked as such.

## Download and build QLever using `docker`

We build QLever using the `docker` container engine, which takes care of all the
necessary dependencies. You can learn more about `docker` at
[docker.com](https://www.docker.com).
Instructions on setting it up on Ubuntu are provided
[here](https://docs.docker.com/install/linux/docker-ce/ubuntu/).

To download QLever we will clone the `git` repository from GitHub. As we create
the QLever index in a subfolder of the repository, **you should make sure, that
you have about 2 TB of space available** on the filesystem on which you execute
the following steps.

Alternatively, you can see the full
[README](https://github.com/ad-freiburg/QLever#building-the-index) on how to
build the index under a different path.

    git clone --recursive https://github.com/ad-freiburg/QLever.git qlever
    cd qlever
    docker build -t qlever .

## Download Wikidata

If you already downloaded Wikidata in the bzip2-compressed
Turtle format, you can skip this step. Otherwise we will download it in this step.

**Note:** This takes several hours as Wikidata is about 42 GB compressed and
their servers are throttled. In case you are in a hurry and don't need to save a copy of the original input
(e.g. for reproducibility), you can directly pipe the file
into the index-building pipeline (see next section).

    mkdir wikidata-input
    wget -O wikidata-input/latest-all.ttl.bz2 \
      https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.ttl.bz2

## Build a QLever Index

Now we can build a QLever Index from the `latest-all.ttl.bz2` Wikidata Turtle file.
For the process of building an index we can tune some settings to the particular
Knowledge Base. The most important of these is a list of relations which can safely be
stored on disk, as their actual values are rarely accessed. For Wikidata these
are all statements as their values are long string ids. Note that this does not
affect the performance of using statements in intermediate results.

These recommended tuning settings are provided in the `wikidata_settings.json`
file.  If you're using a different path for the `latest-all.ttl` file
**you must make sure that the settings file is accessible from within the
container**.

**Note:** If you are using `docker` with user namespaces or your user id (`id
-u`) is not 1000 you have to make the `./index` folder writable for QLever
inside the container e.g. by running `chmod -R o+rw ./index`

    docker run -it --rm \
        -v "$(pwd)/wikidata-input/:/input" \
        -v "$(pwd)/index:/index" --entrypoint "bash" qlever
    qlever@xyz:/app$ bzcat /input/latest-all.ttl.bz2 | IndexBuilderMain -l -i /index/wikidata-full \
        -f - \
        -s /input/wikidata_settings.json
    … wait for about half a day …
    qlever@xyz:/app$ exit

In case you don't want to download and save the `.ttl.bz2` file first the second step becomes

    qlever@xyz:/app$ wget -O - https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.ttl.bz2 \
                   | bzcat /input/latest-all.ttl.bz2 
                   | IndexBuilderMain -l -i /index/wikidata-full -f - \
                                      -s /input/wikidata_settings.json
        
## Run QLever

Finally we are ready to launch a QLever instance using the newly build Wikidata
index.

    docker run -it -p 7001:7001 \
        -v "$(pwd)/index:/index" \
        -e INDEX_PREFIX=wikidata-full \
        --name qlever \
        qlever

Then open [http://localhost:7001/](http://localhost:7001/) in your browser.

For example, the following query retrieves all mountains above 8000 m

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
