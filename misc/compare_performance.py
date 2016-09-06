import argparse
import sys
import subprocess

__author__ = 'buchholb'

virtuoso_run_binary = "/home/buchholb/virtuoso/bin/isql"
virtuoso_isql_port = "1111"
virtuso_isql_user = "dba"
rdf3x_run_binary = "/home/buchholb/rdf3x-0.3.8/bin/rdf3xquery"
rdf3x_db = "/local/scratch/bjoern/data/rdf3x//wikipedia-freebase.combined.db.2"
my_index = "/local/scratch/bjoern/data/wikipedia-freebase.2016-09-02"
my_binary = "/local/scratch/bjoern/work/tmp/SparqlEngineMain"

parser = argparse.ArgumentParser()

parser.add_argument('--queryfile',
                    type=str,
                    help='One line per (standard) SPARQL query. TS specific transformations are made by the python script.',
                    required=True)

parser.add_argument('--virtuoso-pwd',
                    type=str,
                    help='Password for the (already running) virtuoso instance',
                    required=True)


def expanded_to_my_syntax(q):
    before_where, after_where = q.split('WHERE')
    no_mod, mod = after_where.strip().split('}')
    clauses = no_mod.strip('{').split('.')
    new_clauses = []
    context_to_words = {}
    for c in clauses:
        if '<word:' in c:
            try:
                s, p, o = c.strip().split(' ')
            except ValueError:
                print("Problem in : " + c, file=sys.stderr)
                exit(1)
            if o not in context_to_words:
                context_to_words[o] = []
            context_to_words[o].append(s[6: -1])
        else:
            new_clauses.append(c)
    for c, ws in context_to_words.items():
        new_clauses.append(' ' + c + ' <in-context> ' + ' '.join(ws) + ' ')
    new_after_where = ' {' + '.'.join(new_clauses) + '}' + mod
    return 'WHERE'.join([before_where, new_after_where])


def rewrite_filter_for_rdf3x(q):
    before_where, after_where = q.split('WHERE')
    no_mod, mod = after_where.strip().split('}')
    clauses = no_mod.strip('{').split('.')
    new_clauses = []
    for c in clauses:
        if 'FILTER' in c and '!=' in c:
            e1 = c[c.find('?'): c.find('!=')].strip()
            e2 = c[c.find('!=') + 2: c.find(')')].strip()
            new_clauses.append(' FILTER(!(' + e1 + '=' + e2 + ')) ')
        else:
            new_clauses.append(c)
    if 'OFFSET' in mod:
        print('WARNING: IGNORING OFFSET CLAUSE FOR RDF3X!\n', file=sys.stderr)
        tokens = mod.split(' ')
        mod = ''
        i = 0
        while i < len(tokens):
            if tokens[i] == 'OFFSET':
                i += 2
            else:
                mod += tokens[i] + ' '
                i += 1
    new_after_where = ' {' + '.'.join(new_clauses) + '}' + mod
    return 'WHERE'.join([before_where, new_after_where])


def get_virtuoso_query_times(query_file, pwd):
    with open('__tmp.virtqueries', 'w') as tmpfile:
        for line in open(query_file):
            tmpfile.write('SPARQL ' + line.strip().split('\t')[1] + ';\n')
    virtout = subprocess.check_output(
        [virtuoso_run_binary, virtuoso_isql_port, virtuso_isql_user, pwd,
         '__tmp.virtqueries']).decode('utf-8')
    coutfile = open('__tmp.cout.virtuoso', 'w')
    coutfile.write(virtout)
    coutfile.write('\n\n\n\n')
    times = []
    nof_matches = []
    last_i = 0
    for line in virtout.split('\n'):
        i = line.find('Rows. --')
        if i >= 0:
            j = line.find('msec')
            times.append(line[i + 9: j + 2])
            nof_matches.append(line[:i])
            last_i = i
            # os.remove('__tmp.rdf3xqueries')
        queries = []
    for line in open(query_file):
        queries.append(line.strip())
    if len(times) != len(queries) or len(times) != len(nof_matches):
        print('PROBLEM PROCESSING VIRTUOSO: q:' + str(len(queries)) + ' t:' +
                str(len(times)) + ' #:' + str(len(nof_matches)), file=sys.stderr)
    return times, nof_matches


def get_rdf3X_query_times(query_file):
    with open('__tmp.rdf3xqueries', 'w') as tmpfile:
        for line in open(query_file):
            tmpfile.write(
                rewrite_filter_for_rdf3x(line.strip().split('\t')[1]) + '\n')
    rdf3xout = subprocess.check_output(
        [rdf3x_run_binary, rdf3x_db, '__tmp.rdf3xqueries']).decode('utf-8')
    coutfile = open('__tmp.cout.rdf3x', 'w')
    coutfile.write(rdf3xout)
    coutfile.write('\n\n\n\n')
    times = []
    nof_matches = []
    last_i = 0
    for line in rdf3xout.split('\n'):
        i = line.find('Done. Time: ')
        if i >= 0:
            j = line.find('ms')
            times.append(line[i + 9: j + 2])
            nof_matches.append(i - 1 - last_i)
            last_i = i
    # os.remove('__tmp.rdf3xqueries')
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    if len(times) != len(queries) or len(times) != len(nof_matches):
        print('PROBLEM PROCESSING RDF3X: q:' + str(len(queries)) + ' t:' +
                str(len(times)) + ' #:' + str(len(nof_matches)), file=sys.stderr)
    return times, nof_matches


def get_my_query_times(query_file):
    with open('__tmp.myqueries', 'w') as tmpfile:
        for line in open(query_file):
            try:
                tmpfile.write(
                    expanded_to_my_syntax(line.strip().split('\t')[1]) + '\n')
            except IndexError:
                print("Problem with tabs in : " + line)
                exit(1)
    coutfile = open('__tmp.cout.mine', 'w')
    myout = subprocess.check_output(
        [my_binary, '-i', my_index, '-t', '--queryfile', '__tmp.myqueries']).decode('utf-8')
    coutfile.write(myout)
    coutfile.write('\n\n\n\n')
    times = []
    nof_matches_no_limit = []
    nof_matches_limit = []
    for line in myout.split('\n'):
        i = line.find('Done. Time: ')
        if i >= 0:
            j = line.find('ms')
            times.append(line[i + 12: j + 2])
        i = line.find('Number of matches (no limit): ')
        if i >= 0:
            nof_matches_no_limit.append(line[i + 30:])
        i = line.find('Number of matches (limit): ')
        if i >= 0:
            nof_matches_limit.append(line[i + 27:])
    # os.remove('__tmp.myqueries')
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    if len(times) != len(queries) or len(times) != len(nof_matches_limit):
        print('PROBLEM PROCESSING MINE: q:' + str(len(queries)) + ' t:' +
                str(len(times)) + ' #:' + str(len(nof_matches_limit)), file=sys.stderr)
    return times, nof_matches_limit


def processQueries(query_file, pwd):
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    virtuoso_times, virtuoso_counts = get_virtuoso_query_times(query_file, pwd)
    rdf3x_times, rdf3x_counts = get_rdf3X_query_times(query_file)
    my_times, my_counts = get_my_query_times(query_file)
    return queries, virtuoso_times, virtuoso_counts, rdf3x_times, rdf3x_counts, my_times, my_counts


def print_result_table(queries, virtuoso_times, virtuoso_counts, rdf3x_times,
                       rdf3x_counts, my_times, my_counts):
    #assert len(queries) == len(virtuoso_times)
    #assert len(queries) == len(rdf3x_times)
    #assert len(queries) == len(my_times)
    print("\t".join(['id', 'query', 'virtuoso', 'rdf3x', 'mine']))
    print("\t".join(['----', '-----', '-----', '-----', '-----']))
    for i in range(0, len(queries)):
        print(
            "\t".join(
                [queries[i], virtuoso_times[i], rdf3x_times[i], my_times[i]]))
        if virtuoso_counts[i] != rdf3x_counts[i] or virtuoso_counts[i] != \
                my_counts[
                    i]:
            print('DIFFERENT COUTNS FOR QUERY: ' + queries[i] + ': ' +
                  str(virtuoso_counts[i]) + ' vs ' + str(rdf3x_counts[i]) + ' vs '
                  + str(my_counts[i]),
                  file=sys.stderr)


def main():
    args = vars(parser.parse_args())
    queries = args['queryfile']
    queries, virtuoso_times, virtuoso_counts, rdf3x_times, rdf3x_counts, my_times, my_counts = processQueries(queries, args['virtuoso_pwd'])
    print_result_table(queries, virtuoso_times, virtuoso_counts, rdf3x_times,
                       rdf3x_counts, my_times, my_counts)


if __name__ == '__main__':
    main()
