#!/usr/bin/env python3
"""
QLever Query Tool for End2End Testing
"""

import sys
import urllib.parse
import urllib.request
from typing import Dict, Any, List
import json
import yaml
import icu
import dateutil
import dateutil.parser
import datetime
import decimal


class Color:
    """
    Enum-like class for storing ANSI Color Codes
    """
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


datatypes = ["string", "integer", "decimal", "float", "double", "boolean",
             "dateTime", "nonPositiveInteger", "positiveInteger", "negativeInteger",
             "nonNegativeInteger", "long", "int", "short", "byte", "unsignedLong",
             "unsignedInt", "unsignedShort", "unsignedByte"]

double_m_max = pow(2, 53)
double_e_max = 970
double_e_min = -1075

float_m_max = pow(2, 24)
float_e_max = 104
float_e_min = -149

min_long = -9223372036854775808
max_long = 9223372036854775807

min_int = -2147483648
max_int = 2147483647

min_short = -32768
max_short = 32767

min_byte = -128
max_byte = 127

max_u_long = 18446744073709551615
max_u_int = 4294967295
max_u_short = 65535
max_u_byte = 255


def parse_datatype(entry: Any, datatype: str, forceCast: bool) -> Any:
    if datatype == "string":
        if forceCast:
            entry = str(entry)
        if isinstance(entry, str):
            return entry
    elif datatype == "integer":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int):  # Integers in Python 3 are of unlimited size
            return entry
        else:
            return None
    elif datatype == "decimal":
        if forceCast:
            entry = decimal.Decimal(entry)
        if isinstance(entry, decimal.Decimal):
            return entry
    # in python3 float is actually double
    # To be completely compliant with W3C standards it is preferred to confirm it exhaustively [float: IEEE]
    elif datatype == "float":  # IEEE 754-1985
        if forceCast:
            entry = decimal.Decimal(entry)
        if isinstance(entry, decimal.Decimal):
            (sign, digits, exponent) = decimal.Decimal(entry).as_tuple()
            exp = len(digits) + exponent - 1
            man = decimal.Decimal(entry).scaleb(-exp).normalize()
            if float_e_min <= exp <= float_e_max and man <= float_m_max:
                return entry
            else:
                return None
        else:
            return None
    # python3 float is actually double
    # To be completely compliant with W3C standards it is preferred to confirm it exhaustively [double: IEEE]
    elif datatype == "double":  # IEEE 754-1985
        if forceCast:
            entry = decimal.Decimal(entry)
        if isinstance(entry, decimal.Decimal):
            (sign, digits, exponent) = decimal.Decimal(entry).as_tuple()
            exp = len(digits) + exponent - 1
            man = decimal.Decimal(entry).scaleb(-exp).normalize()
            if double_e_min <= exp <= double_e_max and man <= double_m_max:
                return entry
            else:
                return None
        else:
            return None
    elif datatype == "boolean":
        if forceCast:
            entry = bool(entry)
        if isinstance(entry, bool):
            return entry
        else:
            return None
    elif datatype == "dateTime":
        if forceCast:
            return dateutil.parser.parse(entry)
        if isinstance(entry, datetime.date):
            return entry
        elif isinstance(entry, datetime.datetime):
            return entry
        elif isinstance(entry, datetime.time):
            return entry
        elif isinstance(entry, datetime.timedelta):
            return entry
        elif isinstance(entry, datetime.tzinfo):
            return entry
        elif isinstance(entry, datetime.timezone):
            return entry
        else:
            return None
    elif datatype == "nonPositiveInteger":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int) and int(entry) <= 0:
            return entry
        else:
            return None
    elif datatype == "positiveInteger":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int) and int(entry) > 0:
            return entry
    elif datatype == "negativeInteger":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int) and int(entry) <= 0:
            return entry
        else:
            return None
    elif datatype == "nonNegativeInteger":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int) and int(entry) >= 0:
            return entry
        else:
            return None
    elif datatype == "long":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int) and min_long <= int(entry) <= max_long:
            return entry
        else:
            return None
    elif datatype == "int":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int) and min_int <= int(entry) <= max_int:
            return entry
        else:
            return None
    elif datatype == "short":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int) and min_short <= int(entry) <= max_short:
            return entry
        else:
            return None
    elif datatype == "byte":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int) and min_byte <= int(entry) <= max_byte:
            return entry
        else:
            return None
    elif datatype == "unsignedLong":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int) and 0 <= int(entry) <= max_u_long:
            return entry
        else:
            return None
    elif datatype == "unsignedInt":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int) and 0 <= int(entry) <= max_u_int:
            return entry
        else:
            return None
    elif datatype == "unsignedShort":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int) and 0 <= int(entry) <= max_u_short:
            return entry
        else:
            return None
    elif datatype == "unsignedByte":
        if forceCast:
            entry = int(entry)
        if isinstance(entry, int) and 0 <= int(entry) <= max_u_byte:
            return entry
        else:
            return None
    else:
        return None


def eprint(*args, color=Color.FAIL, **kwargs):
    sys.stdout.write(color)
    print(*args, **kwargs)
    print(Color.ENDC)


def exec_query(endpoint_url: str, sparql: str, action,
               max_send: int = 4096) -> [None, Dict[str, Any]]:
    """
    Execute a single SPARQL query against the given endpoint
    """
    params = urllib.parse.urlencode({'query': sparql, 'send': max_send, 'action': action})
    url_suffix = '/?' + params
    request = urllib.request.Request(endpoint_url + url_suffix)
    conn = urllib.request.urlopen(request)
    if conn.status != 200:
        eprint("Error executing SPARQL Query: ", sparql)
        return None
    return json.load(conn)


def check_keys_in_result(result: Dict[str, Any], required_keys: List[str]) -> bool:
    for key in required_keys:
        if key not in result:
            eprint('QLever Result is missing "%s" field' % key)
            return False
    return True


def check_structure_qlever_json(result: Dict[str, Any]) -> bool:
    """
    Checks a QLever Result object for sanity
    """
    return check_keys_in_result(result, ['query', 'status', 'resultsize', 'selected', 'res'])


def check_structure_sparql_json(result: Dict[str, Any]) -> bool:
    """
    Checks a sparql-results+json object for sanity
    """
    if not check_keys_in_result(result, ['head', 'results']):
        return False
    if not check_keys_in_result(result['head'], ['vars']):
        return False
    if not check_keys_in_result(result['results'], ['bindings']):
        return False
    return True


def check_row_sparql_json(variables: List[str], gold_row: List[Any], actual_row: Dict[str, any],
                          row_datatype: List[str], epsilon=0.1) -> bool:
    """
    Test if gold_row and actual_row match. For floats we allow an epsilon
    difference. If a gold_row cell is None it is ignored.
    Returns True if they match
    """
    matches = None
    for i, gold in enumerate(gold_row):
        if gold is None:
            continue
        var = variables[i]
        if var not in actual_row and var.startswith("TEXT("):
            var = var[5:-1]
        if var not in actual_row:
            eprint("{} not contained in row {}".format(var, actual_row))
            return False
        actual = actual_row[var]
        # from literals only take the part in quotes stripping
        # the quotes and any "^^xsd:type hints.
        # This allows us to ignore double quoting trouble in checks
        target_type = "literal"
        if isinstance(gold, str) and gold.startswith('<'):
            target_type = "iri"
            gold = gold[1:-1]
        if not actual["type"] == target_type:
            return False
        multiple_datatype = row_datatype[i].split("|")
        for muliple_value_parsed in multiple_datatype:
            value_parsed = None
            try:
                value_parsed = parse_datatype(gold, muliple_value_parsed, True)
            except Exception as e:
                eprint("\tException = " + str(e))
            finally:
                if value_parsed:
                    if muliple_value_parsed != "string" and "datatype" in actual:
                        actual_value = actual["value"]
                        actual_type = actual["datatype"][actual["datatype"].rfind('#') + 1:]
                        parsed_actual = parse_datatype(actual_value, actual_type, True)
                        matches = value_parsed == parsed_actual and muliple_value_parsed == actual_type
                        if not muliple_value_parsed == actual_type:
                            eprint("\trow_datatype = " + muliple_value_parsed + "\n\tactual_type = " + actual_type + "\n")
                    else:
                        matches = value_parsed == actual["value"]  # empty values or string
                else:
                    matches = gold == actual["value"]  # empty values
                if matches:
                    break
        if not matches:
            return False
    return True


def check_row_qlever_json(gold_row: List[Any], actual_row: List[Any],
                          row_datatype: List[str], epsilon=0.1) -> bool:
    """
    Test if gold_row and actual_row match. For floats we allow an epsilon
    difference. If a gold_row cell is None it is ignored.
    Returns True if they match
    """
    if len(gold_row) != len(actual_row):
        return False
    for i, gold in enumerate(gold_row):
        if gold is None:
            continue
        actual = actual_row[i]
        # from literals only take the part in quotes stripping
        # the quotes and any "^^xsd:type hints.
        # This allows us to ignore double quoting trouble in checks
        if actual and actual[0] == '"':
            actual = actual[1:actual.rfind('"')]
        matches = None
        # '''
        if isinstance(gold, int):
            matches = int(actual) == gold
        elif isinstance(gold, float):
            matches = abs(gold - float(actual)) <= epsilon
        else:
            matches = gold == actual
        # '''

        if not matches:
            return False
    return True


def quotes_inner(quoted: str) -> str:
    """
    For a string containing a quoted part returns the inner part
    """
    left_quote = quoted.find('"')
    right_quote = quoted.rfind('"')
    if right_quote < 0:
        right_quote = len(quoted)
    return quoted[left_quote + 1:right_quote]


def test_check_qlever_json(check_dict: Dict[str, Any], result: Dict[str, Any], row_data_type: List[str]) -> (bool, str):
    """
    Test if the named result check holds. Returns True if it does
    """

    collator = icu.Collator.createInstance(icu.Locale('de_DE.UTF-8'))
    res = result['res']
    for check, value in check_dict.items():
        if check == 'num_rows':
            if len(res) != int(value):
                eprint("num_rows check failed:\n" +
                       "\texpected %r, got %r" %
                       (value, len(res)))
                return False, row_data_type
        elif check == 'num_cols':
            for row in res:
                if len(row) != int(value):
                    eprint("num_cols check failed:\n" +
                           "\texpected %r, got %r, row: %s" %
                           (value, len(row), json.dumps(row)))
                    return False, row_data_type
        elif check == 'selected':
            if value != result['selected']:
                eprint("selected check failed:\n" +
                       "\texpected %r, got %r" %
                       (value, result['selected']))
                return False, row_data_type
        elif check == 'row_data_types':
            for column_data_type in value:
                multiple_datatype = column_data_type.split("|")
                for mul_datatype in multiple_datatype:
                    if mul_datatype not in datatypes:
                        eprint("row_data_types check failed:\n" +
                               "\tunexpected data type \'" + mul_datatype + "\'")
                        return False, row_data_type
                row_data_type.append(column_data_type)
        elif check == 'res':
            gold_res = value
            if len(gold_res) != len(res):
                eprint("res check failed:\n" +
                       "\texpected number of rows: %r" % len(gold_res) +
                       "\tdoes not match actual: %r" % len(value))
                return False, row_data_type
            for i, gold_row in enumerate(gold_res):
                actual_row = res[i]
                if not check_row_qlever_json(gold_row, actual_row, row_data_type):
                    eprint("res check failed:\n" +
                           "\tat row %r" % i +
                           "\n\t\texpected: %r \n\t\tgot: %r" %
                           (gold_row, actual_row))
                    return False, row_data_type
        elif check == 'contains_row':
            found = False
            gold_row = value
            for actual_row in res:
                if check_row_qlever_json(gold_row, actual_row, row_data_type):
                    found = True
                    break
            if not found:
                eprint("contains_row check failed:\n" +
                       "\tdid not find %r" % gold_row)
                return False, row_data_type
        elif check == 'contains_warning':
            for requested_warning in value:
                found = False
                for actual_warning in result["warnings"]:
                    if actual_warning.find(requested_warning):
                        found = True
                        break
                if not found:
                    eprint("contains_warning check failed:\n" +
                           "\tdid not find %r" % requested_warning)
                    eprint("actual warnings:", result["warnings"])
                    return False, row_data_type
        elif check.startswith('order_'):
            try:
                direction, var = value['dir'], value['var']
                col_type = check.split('_')[1]
                col_idx = result['selected'].index(var)
                for row_idx in range(1, len(res)):
                    previous = res[row_idx - 1][col_idx]
                    current = res[row_idx][col_idx]
                    if col_type == 'numeric':
                        previous_value = float(quotes_inner(previous))
                        current_value = float(quotes_inner(current))
                    elif col_type == 'string':
                        previous_value = collator.getSortKey(previous)
                        current_value = collator.getSortKey(current)
                    if direction.lower() == 'asc' and previous_value > current_value:
                        eprint('order_numeric check failed:\n\tnot ascending for {} and {}'.format(previous_value,
                                                                                                   current_value))
                        return False, row_data_type
                    if direction.lower() == 'desc' and previous_value < current_value:
                        eprint('order_numeric check failed:\n\tnot ascending for {} and {}'.format(previous_value,
                                                                                                   current_value))
                        return False, row_data_type
            except ValueError as ex:
                eprint('order_numeric check failed:\n\t' + str(ex))
                return False, row_data_type
        else:
            eprint("Unexpected check : " + check)
            return False, row_data_type

    return True, row_data_type


def test_check_sparql_json(check_dict: Dict[str, Any], result: Dict[str, Any], row_data_type: List[str]) -> (bool, str):
    """
    Test if the named result check holds. Returns True if it does
    """

    collator = icu.Collator.createInstance(icu.Locale('de_DE.UTF-8'))
    res = result['results']['bindings']
    for check, value in check_dict.items():
        if check == 'num_rows':
            if len(res) != int(value):
                eprint("num_rows check failed:\n" +
                       "\texpected %r, got %r" %
                       (value, len(res)))
                return False, row_data_type
        elif check == 'num_cols':
            # undefined variables don't create bindings,
            # so this check is not really possible for the
            # sparql json
            pass
        elif check == 'selected':
            if value != result['head']['vars']:
                eprint("selected check failed:\n" +
                       "\texpected %r, got %r" %
                       (value, result['selected']))
                return False, row_data_type
        elif check == 'row_data_types':
            for column_data_type in value:
                multiple_datatype = column_data_type.split("|")
                for mul_datatype in multiple_datatype:
                    if mul_datatype not in datatypes:
                        eprint("row_data_types check failed:\n" +
                               "\tunexpected data type \'" + mul_datatype + "\'")
                        return False, row_data_type
                row_data_type.append(column_data_type)
        elif check == 'res':
            gold_res = value
            if len(gold_res) != len(res):
                eprint("res check failed:\n" +
                       "\texpected number of rows: %r" % len(gold_res) +
                       "\tdoes not match actual: %r" % len(value))
                return False, row_data_type
            for i, gold_row in enumerate(gold_res):
                actual_row = res[i]
                if not check_row_sparql_json(result["head"]["vars"], gold_row, actual_row, row_data_type):
                    eprint("res check failed:\n" +
                           "\tat row %r" % i +
                           "\n\t\texpected: %r \n\t\tgot: %r" %
                           (gold_row, actual_row))
                    return False, row_data_type
        elif check == 'contains_row':
            found = False
            gold_row = value
            for actual_row in res:
                if check_row_sparql_json(result["head"]["vars"], gold_row, actual_row, row_data_type):
                    found = True
                    break
            if not found:
                eprint("contains_row check failed:\n" +
                       "\tdid not find %r" % gold_row)
                return False, row_data_type
        elif check == 'contains_warning':
            # currently the sparql_json contains no warnings
            # TODO<joka921> is there any harm in adding them?
            pass
        elif check.startswith('order_'):
            # The order is already checked with the qlever_json checks.
            pass
        else:
            eprint("Unexpected check : " + check)
            return False, row_data_type

    return True, row_data_type


def query_checks(query: Dict[str, Any],
                 result: Dict[str, Any], test_check_method) -> bool:
    """
    Tests the checks specified in the query
    """
    if not 'checks' in query:
        return True
    passed = True
    checks = query['checks']
    row_data_type = []
    for check in checks:
        res = test_check_method(check, result, row_data_type)
        if not res[0]:
            passed = False
        elif len(row_data_type) == 0:
            row_data_type = res[1]  # retrieve row_data_type for row_check
    return passed


def print_qlever_result(result: Dict[str, Any]) -> None:
    """
    Prints a QLever Result to stdout
    """
    eprint(json.dumps(result))


def main() -> None:
    """
    Run QLever queries stored in a YAML file against a QLever instance
    """
    if len(sys.argv) != 3:
        eprint("Usage: ", sys.argv[0], "<yaml_in> <qlever_endpoint_url>")
        sys.exit(1)

    inpath = sys.argv[1]
    endpoint_url = sys.argv[2]
    error_detected = False
    with open(inpath, 'rb') if inpath != '-' else sys.stdin as infile:
        yaml_tree = yaml.safe_load(infile)
        queries = yaml_tree['queries']
        for query in queries:
            query_name = query['query']
            query_type = query['type']
            query_sparql = query['sparql']
            print(Color.HEADER + 'Trying: ', query_name,
                  '(%s)' % query_type + Color.ENDC)
            print('SPARQL:')
            print(query_sparql)
            sys.stdout.flush()
            result = exec_query(endpoint_url, query_sparql, action="qlever_json_export")
            if not result:
                # A print was already done in exec_query()
                error_detected = True
                print_qlever_result(result)
                continue

            if not check_structure_qlever_json(result):
                error_detected = True
                print_qlever_result(result)
                continue

            if result['status'] != 'OK':
                eprint('QLever Result "status" is not "OK"')
                error_detected = True
                print_qlever_result(result)
                continue

            if not query_checks(query, result, test_check_qlever_json):
                error_detected = True
                continue

            result = exec_query(endpoint_url, query_sparql, action="sparql_json_export")
            if not result:
                # A print was already done in exec_query()
                error_detected = True
                print_qlever_result(result)
                continue

            if not check_structure_sparql_json(result):
                error_detected = True
                print_qlever_result(result)
                continue

            if not query_checks(query, result, test_check_sparql_json):
                error_detected = True
                continue

    if error_detected:
        print(Color.FAIL + 'Query tool found errors!' + Color.ENDC)
        sys.exit(2)

    print(Color.OKGREEN + 'Query tool did not find errors, search harder!' + Color.ENDC)


if __name__ == '__main__':
    print("Python {:s} on {:s}\n".format(sys.version, sys.platform))
    main()
