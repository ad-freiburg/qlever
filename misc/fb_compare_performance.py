import argparse
import sys
import subprocess
import json, requests


__author__ = 'buchholb'

virtuoso_run_binary = "/home/buchholb/virtuoso/bin/isql"
virtuoso_isql_port = "1111"
bifc_isql_port = "1113"
bifc_inc_isql_port = "1114"
virtuso_isql_user = "dba"
rdf3x_run_binary = "/home/buchholb/rdf3x-0.3.8/bin/rdf3xquery"
rdf3x_db = "/local/raid/ad/buchholb/eval/rdf3x/wikipedia-freebase.combined.db"
my_index = "/local/raid/ad/buchholb/clueweb-freebase/clueweb-freebase"
my_binary = "/home/buchholb/SparqlEngine/build/SparqlEngineMain"
broccoli_api = 'http://localhost:6001/'

parser = argparse.ArgumentParser()

parser.add_argument('--queryfile',
                    type=str,
                    help='One line per (standard) SPARQL query. TS specific transformations are made by the python script.',
                    required=True)

parser.add_argument('--virtuoso-pwd',
                    type=str,
                    help='Password for the (already running) virtuoso instance',
                    required=True)

parser.add_argument('--all', action='store_true', help='should all be executed?',
                    default=False)
parser.add_argument('--virt', action='store_true', help='should virt be executed?',
                    default=False)
parser.add_argument('--bifc', action='store_true', help='should bifc be executed?',
                    default=False)
parser.add_argument('--bifc-inc', action='store_true', help='should bifc_inc be executed?',
                    default=False)
parser.add_argument('--mine', action='store_true', help='should mine be executed?',
                    default=False)
parser.add_argument('--rdf3x', action='store_true', help='should rdf3x be executed?',
                    default=False)
parser.add_argument('--broccoli', action='store_true', help='should broccoli be executed?',
                    default=False)

parser.add_argument('--names', action='store_true', help='Should I select names for all variables?',
                    default=False)


def expanded_to_my_syntax(q):
    try:
        before_where, after_where = q.split('WHERE')
        no_mod, mod = after_where.strip().split('}')
        clauses = no_mod.strip('{').split('.')
        new_clauses = []
        context_to_words = {}
        for c in clauses:
            if '<word:' in c:
                try:
                    s, p, o = c.strip().split(' ')
                    if o not in context_to_words:
                        context_to_words[o] = []
                    context_to_words[o].append(s[6: -1])
                except ValueError:
                    print("Problem in : " + c, file=sys.stderr)
                    exit(1)
            else:
                new_clauses.append(c)
        for c, ws in context_to_words.items():
            new_clauses.append(' ' + c + ' <in-context> ' + ' '.join(ws) + ' ')
        new_after_where = ' {' + '.'.join(new_clauses) + '}' + mod
        return 'WHERE'.join([before_where, new_after_where])
    except:
        print("Problem in : " + q, file=sys.stderr)


def add_getting_names(q):
    try:
        before_where, after_where = q.split('WHERE')
        vs = before_where.split('SELECT')[1].strip().lstrip('DISTINCT').lstrip().split(' ')
        no_mod, mod = after_where.strip().split('}')
        clauses = no_mod.strip('{').split('.')
        new_clauses = []
        for v in vs:
            if v == '?c' or v == '?c2' or v.startswith('?val_'):
                continue
            before_where = before_where.replace(v, v + ' ' + v + 'name')
            new_clauses.append(' ' + v + ' fb:type.object.name.en  ' + v + 'name')
        context_to_words = {}
        for c in clauses:
            if '<word:' in c:
                try:
                    s, p, o = c.strip().split(' ')
                    if o not in context_to_words:
                        context_to_words[o] = []
                    context_to_words[o].append(s[6: -1])
                except ValueError:
                    print("Problem in : " + c, file=sys.stderr)
                    exit(1)
            else:
                new_clauses.append(c)
        for c, ws in context_to_words.items():
            new_clauses.append(' ' + c + ' <in-context> ' + ' '.join(ws) + ' ')
        new_after_where = ' {' + '.'.join(new_clauses) + '}' + mod
        return 'WHERE'.join([before_where, new_after_where])
    except:
        e = sys.exc_info()[0]
        print("Exception " + e, file=sys.stderr)
        print("Problem in input : " + q, file=sys.stderr)


def get_my_query_times(query_file, get_names):
    print('get_my_query_times', file=sys.stderr)
    with open('__tmp.my_queries', 'w') as tmpfile:
        for line in open(query_file):
            try:
                q = line.strip().split('\t')[1]
                if get_names:
                    q = add_getting_names(q)
                tmpfile.write(expanded_to_my_syntax(q) + '\n')
            except IndexError:
                print("Problem with tabs in : " + line)
                exit(1)
    coutfile = open('__tmp.cout.mine', 'w')
    myout = subprocess.check_output(
        [my_binary, '-i', my_index, '-t', '--queryfile', '__tmp.my_queries']).decode('utf-8')
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
            nof_matches_no_limit.append(int(line[i + 30:]))
        i = line.find('Number of matches (limit): ')
        if i >= 0:
            nof_matches_limit.append(int(line[i + 27:]))
    # os.remove('__tmp.my_queries')
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    if len(times) != len(queries) or len(times) != len(nof_matches_limit):
        print('PROBLEM PROCESSING MINE: q:' + str(len(queries)) + ' t:' +
                str(len(times)) + ' #:' + str(len(nof_matches_limit)), file=sys.stderr)
    return times, nof_matches_limit


def print_result_table(queries, headers, time_and_counts):
    print('\t'.join(headers))
    print('\t'.join(['---'] * len(headers)))
    data = []
    for i in range(2, len(headers)):
        data.append([])
    for i in range(0, len(queries)):
        row = [queries[i]]
        approach_num = 0
        for (times, counts) in time_and_counts:
            row.append(str(times[i]))
            row.append(str(counts[i]))
            if times[i] != '-':
                data[approach_num * 2].append(float(times[i].strip().strip('ms').strip()))
                data[approach_num * 2 + 1].append(float(counts[i]))
            approach_num += 1
        print('\t'.join(row))
    print('\t'.join(['average', ' '] + ['{0:.2f}'.format((sum(l) / len(l))) for l in data]))


def main():
    args = vars(parser.parse_args())
    query_file = args['queryfile']
    queries = []
    for line in open(query_file):
        queries.append(line.strip())
    all = args['all']
    virt = args['virt']
    bifc = args['bifc']
    bifc_inc = args['bifc_inc']
    rdf3x = args['rdf3x']
    mine = args['mine']
    broccoli = args['broccoli']
    headers = []
    headers.append('id')
    headers.append('query')
    time_and_counts = []
    if all or mine:
        headers.append('mine')
        headers.append('#m_mine')
        times, matches = get_my_query_times(query_file, args['names'])
        time_and_counts.append((times, matches))
    print_result_table(queries, headers, time_and_counts)


if __name__ == '__main__':
    main()
