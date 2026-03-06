SHELL = /bin/bash
.DEFAULT_GOAL = show-config

# PIN WARMUP QUERIES TO CACHE (for the QLever UI)
# (c) Algorithms and Data Structures, University of Freiburg
# Originally written by Hannah Bast, 20.02.2021

# This Makefile provides the following targets:
#
# pin: Pin queries to cache, so that all autocompletion queries are fast, even
#      when "Clear cache" is clicked in the QLever UI (the results for pinned 
#      queries will never be removed, unless ... see target clear).
#
# clear: Clear the cache completely (including pinned results). Note that this
#        can NOT be activated from the QLever UI.
#
# clear-unpinned: Clear all unpinned results from the cache. This is exactly
#                 what happens when clicking "Clear cache" in the QLever UI.
#
# show-all-ac-queries: Show the AC queries for subject, predicate, object for
#                      copy&paste in the QLever UI backend settings.

# This Makefile should be used as follows:
#
# 1. In the directors with the particular index, create a new Makefile
# 2. At the top add: include /local/data/qlever/qlever-indices/Makefile
#    (or wherever this Makefile - the master Makefile - resides)
# 3. Redefine API and FREQUENT_PREDICATES (see below) in the local Makefile
# 4. Redefine any of the patterns in the local Makefile
#    (the patterns below give a default functionality, which should work
#    for any knowledge base, but only using the raw IRIs, and no names,
#    aliases, or whatever special data the knowledge base has to offer.

# A prefix that identifies a particular build. This typically consists of a base
# name and optionally further specificataion separated by dots. For example:
# wikidata. Or: wikidata.2021-06-27
DB = 

# The base name of the prefix = the part before the first dot. Often, the name
# of the input (ttl) file or of the settings (json) file use only the basename
# and not the full prefix.
DB_BASE = $(firstword $(subst ., ,$(DB)))

# The port of the QLever backend.
PORT = 

# The slug used in the URL of the QLever API and the QleverUI API. This is
# typically the basename of the prefix. For example: wikidata. Or: freebase.
SLUG = $(DB_BASE)

# Memory for queries and for the cache (all in GB).
MEMORY_FOR_QUERIES = 30
CACHE_MAX_SIZE_GB = 30
CACHE_MAX_SIZE_GB_SINGLE_ENTRY = 5
CACHE_MAX_NUM_ENTRIES = 1000

# The URL of the QLever backend.
QLEVER_API = https://qlever.cs.uni-freiburg.de/api/$(SLUG)

# The URL of the QLever UI instance ... TODO: it's confusing that this also has
# /api/ in the name, it actually has nothing to do with the URLs from the QLever
# backends (which are defined in the Apache configuration of QLever).
WARMUP_API = $(subst /api/,/api/warmup/,$(QLEVER_API))

# Admin token
TOKEN = aof4Ad

# Frequent predicates that should be pinned to the cache (can be left empty).
# Separate by space. You can use all the prefixes from PREFIXES (e.g. wdt:P31 if
# PREFIXES defines the prefix for wdt), but you can also write full IRIs. Just
# see how it is used in target pin: below, it's very simple.
FREQUENT_PREDICATES =
FREQUENT_PATTERNS_WITHOUT_ORDER =

# The name of the docker image.
DOCKER_IMAGE = qlever.master

# The name of the docker container. Used for target memory-usage: below.
DOCKER_CONTAINER = qlever.$(DB)

# Configuration for SPARQL+Text.
QLEVER_TOOL_DIR = $(dir $(lastword $(MAKEFILE_LIST)))misc
WITH_TEXT =
TEXT_OPTIONS_INDEX = $(if $(WITH_TEXT),-w $(DB).wordsfile.tsv -d $(DB).docsfile.tsv,)
TEXT_OPTIONS_START = $(if $(WITH_TEXT),-t,)

show-config-default:
	@ echo
	@ echo "Basic configuration variables:"
	@ echo
	@ for VAR in DB DB_BASE SLUG CAT_TTL \
	   PORT QLEVER_API WARMUP_API \
	   DOCKER_IMAGE DOCKER_CONTAINER \
	   MEMORY_FOR_QUERIES \
	   CACHE_MAX_SIZE_GB CACHE_MAX_SIZE_GB_SINGLE_ENTRY CACHE_MAX_NUM_ENTRIES \
	   TEXT_OPTIONS_INDEX TEXT_OPTIONS_START; do \
	   printf "%-30s = %s\n" "$$VAR" "$${!VAR}"; done
	@ echo
	@ printf "All targets: "
	@ grep "^[A-Za-z._]\+:" $(lastword $(MAKEFILE_LIST)) | sed 's/://' | paste -sd" "
	@ echo
	@ echo "make index will do the following (but NOT if an index with that name exists):"
	@ echo
	@ $(MAKE) -sn index | egrep -v "^(if|fi)"
	@ echo

%: %-default
	@ true

# Build an index or remove an existing one

CAT_TTL = cat $(DB).ttl

index:
	@if ls $(DB).index.* 1> /dev/null 2>&1; then echo -e "\033[31mIndex exists, delete it first with make remove_index, which would execute the following:\033[0m"; echo; make -sn remove_index; echo; else \
	time ( docker run -it --rm -v $(shell pwd):/index --entrypoint bash --name qlever.$(DB)-index $(DOCKER_IMAGE) -c "cd /index && $(CAT_TTL) | qlever-index -F ttl -f - -l -i $(DB) -K $(DB) $(TEXT_OPTIONS_INDEX) -s $(DB_BASE).settings.json | tee $(DB).index-log.txt"; rm -f $(DB)*tmp* ) \
	fi

remove_index:
	rm -f $(DB).index.* $(DB).vocabulary.* $(DB).prefixes $(DB).meta-data.json $(DB).index-log.txt

# Create wordsfile and docsfile from all literals of the given NT file.
# Using this as input for a SPARQL+Text index build will effectively enable
# keyword search in literals. To understand how, look at the wordsfile and
# docsfile produced. See git:ad-freiburg/qlever/docs/sparql_and_text.md .
text_input_from_nt_literals:
	python3 $(QLEVER_TOOL_DIR)/words-and-docs-file-from-nt.py $(DB)

# START, WAIT (until the backend is read to respond), STOP, and view LOG

start:
	-docker rm -f $(DOCKER_CONTAINER)
	docker run -d --restart=unless-stopped -v $(shell pwd):/index -p $(PORT):7001 -e INDEX_PREFIX=$(DB) -e MEMORY_FOR_QUERIES=$(MEMORY_FOR_QUERIES) -e CACHE_MAX_SIZE_GB=${CACHE_MAX_SIZE_GB} -e CACHE_MAX_SIZE_GB_SINGLE_ENTRY=${CACHE_MAX_SIZE_GB_SINGLE_ENTRY} -e CACHE_MAX_NUM_ENTRIES=${CACHE_MAX_NUM_ENTRIES} --name $(DOCKER_CONTAINER) $(DOCKER_IMAGE) $(TEXT_OPTIONS_START)
	
wait:
	@docker logs -f --tail 10 $(DOCKER_CONTAINER) & PID=$$!; \
	 while [ $$(curl --silent http://localhost:$(PORT) > /dev/null; echo $$?) != 0 ]; \
	   do sleep 1; done; kill $$PID

start_and_pin:
	$(MAKE) -s start wait pin.remote

stop:
	docker stop $(DOCKER_CONTAINER)

log:
	docker logs -f --tail 100 $(DOCKER_CONTAINER)


# WARMUP queries. The .local target only works on the machine, where the
# qlever-ui Docker container is running. It has the advantage of being more
# interactive than the WARMUP_API call (which for pin: returns only after all
# warmup queries have been executed and times out if this takes too long, for
# reasons I have not fully understood yet, apparently there is a time out in one
# of the proxies involved).
HTML2ANSI = jq -r '.log|join("\n")' | sed 's|<strong>\(.*\)</strong>|\\033[1m\1\\033[0m|; s|<span style="color: blue">\(.*\)</span>|\\033[34m\1\\033[0m|' | xargs -0 echo -e

pin.remote:
	ssh -t galera docker exec -it qlever-ui bash -c \"python manage.py warmup $(SLUG) pin\"

pin.local:
	docker exec -it qlever-ui bash -c "python manage.py warmup $(SLUG) pin"

clear.local:
	docker exec -it qlever-ui bash -c "python manage.py warmup $(SLUG) clear"

clear_unpinned.local:
	docker exec -it qlever-ui bash -c "python manage.py warmup $(SLUG) clear_unpinned"

pin:
	@if ! curl -Gsf $(WARMUP_API)/pin?token=$(TOKEN) | $(HTML2ANSI); \
	  then curl -Gi $(WARMUP_API)/pin?token=$(TOKEN); fi

clear:
	@curl -Gs $(WARMUP_API)/clear?token=$(TOKEN) | $(HTML2ANSI)
	@# curl -Gs $(QLEVER_API) --data-urlencode "cmd=clearcachecomplete" > /dev/null

clear_unpinned:
	@curl -Gsf $(WARMUP_API)/clear_unpinned?token=$(TOKEN) | $(HTML2ANSI)
	@# curl -Gs $(QLEVER_API) --data-urlencode "cmd=clearcache" > /dev/null


# STATISTICS on cache, memory, and the number of triples per predicate.

disk_usage:
	du -hc $(DB).index.* $(DB).vocabulary.* $(DB).prefixes $(DB).meta-data.json

cachestats:
	@curl -Gs $(QLEVER_API) --data-urlencode "cmd=cachestats" \
	  | sed 's/[{}]//g; s/:/: /g; s/,/ , /g' | numfmt --field=2,5,8,11,14 --grouping && echo

memory_usage:
	@echo && docker stats --no-stream --format \
	  "Memory usage of docker container $(DOCKER_CONTAINER): {{.MemUsage}}" $(DOCKER_CONTAINER)


num_triples:
	@echo -e "\033[1mCompute total number of triples by computing the number of triples for each predicate\033[0m"
	curl -Gs $(QLEVER_API) --data-urlencode "query=SELECT ?p (COUNT(?p) AS ?count) WHERE { ?x ql:has-predicate ?p } GROUP BY ?p ORDER BY DESC(?count)" --data-urlencode "action=tsv_export" \
	  | cut -f1 | grep -v "QLever-internal-function" \
	  > $(DB).predicates.txt
	cat $(DB).predicates.txt \
	  | while read P; do \
	      $(MAKE) -s clear-unpinned > /dev/null; \
	      printf "$$P\t" && curl -Gs $(QLEVER_API) --data-urlencode "query=SELECT ?x ?y WHERE { ?x $$P ?y }" --data-urlencode "send=10" \
	        | grep resultsize | sed 's/[^0-9]//g'; \
	    done \
	  | tee $(DB).predicate-counts.tsv | numfmt --field=2 --grouping
	cut -f2 $(DB).predicate-counts.tsv | paste -sd+ | bc | numfmt --grouping \
	  | tee $(DB).num-triples.txt


# SETTINGS
settings:
	@curl -Gs $(QLEVER_API) --data-urlencode "cmd=get-settings" \
	  | sed 's/[{}]//g; s/:/: /g; s/,/ , /g' && echo

BB_FACTOR_SORTED = 100
BB_FACTOR_UNSORTED = 150
set_bb:
	@echo -e "\033[1mSet factor for BB FILTER cost estimate to $(BB_FACTOR)\033[0m"
	@curl -Gs $(QLEVER_API) \
	  --data-urlencode "bounding_box_filter_sorted_cost_estimate=$(BB_FACTOR_SORTED)" \
	  --data-urlencode "bounding_box_filter_unsorted_cost_estimate=$(BB_FACTOR_UNSORTED)" \
	  > \dev\null
	@$(MAKE) -s settings
	
export

