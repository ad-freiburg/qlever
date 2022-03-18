#!/usr/bin/env python3

import dateutil
import dateutil.parser
import datetime
import decimal
import numpy
from typing import Any

# https://www.w3.org/TR/rdf-sparql-query/#operandDataTypes
datatypes = ["string", "integer", "decimal", "float", "double", "boolean",
             "dateTime", "nonPositiveInteger", "positiveInteger", "negativeInteger",
             "nonNegativeInteger", "long", "int", "short", "byte", "unsignedLong",
             "unsignedInt", "unsignedShort", "unsignedByte"]

# LONG
min_long = -9223372036854775808
assert -pow(2, 63) == min_long
max_long = 9223372036854775807
assert pow(2, 63)-1 == max_long

# INT
min_int = -2147483648
assert -pow(2, 31) == min_int
max_int = 2147483647
assert pow(2, 31)-1 == max_int

# SHORT
min_short = -32768
assert -pow(2, 15) == min_short
max_short = 32767
assert pow(2, 15)-1 == max_short

# BYTE
min_byte = -128
assert -pow(2, 7) == min_byte
max_byte = 127
assert pow(2, 7)-1 == max_byte

# UNSIGNED
max_u_long = 18446744073709551615
assert pow(2, 64)-1 == max_u_long
max_u_int = 4294967295
assert pow(2, 32)-1 == max_u_int
max_u_short = 65535
assert pow(2, 16)-1 == max_u_short
max_u_byte = 255
assert pow(2, 8)-1 == max_u_byte

# Python3 Integers (int) has no maximum limit in Python3

def cast_datatype(entry: Any, datatype: str, forceCast: bool) -> Any:
    """
    Cast/Test the input variable to the desired datatype

    :param entry: input variable
    :param datatype: input type
    :param forceCast: forces type if true, checks type if false
    :return: [None, datatype] (if it fails then it returns None)
    """
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
    # To be completely compliant with W3C standards it is preferred to cast it to float 32 [float: IEEE]
    elif datatype == "float":  # IEEE 754-1985
        if forceCast:
            entry = numpy.float32(entry)
        if isinstance(entry, numpy.float32):
            return entry
    # python3 float is actually double
    # To be completely compliant with W3C standards it is preferred to cast it to float 64 [double: IEEE]
    elif datatype == "double":  # IEEE 754-1985
        if forceCast:
            entry = numpy.float64(entry)
            if isinstance(entry, numpy.float64):
                return entry
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
