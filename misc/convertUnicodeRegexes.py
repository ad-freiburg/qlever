
def utf8_bytes_required(num):
    if num < 2**7:
        return 1
    if num < 2**11:
        return 2
    if num < 2**16:
        return 3
    return 4

def utf8_min_codepoint_for_num_bytes(num_bytes):
    if num_bytes == 1:
        return 0
    if num_bytes == 2:
        return 2**7
    if num_bytes == 3:
        return 2**11
    if num_bytes == 4:
        return 2**16

def utf8_max_codepoint_for_num_bytes(num_bytes):
    if num_bytes < 4:
        return utf8_min_codepoint_for_num_bytes(num_bytes - 1) - 1
    raise RuntimeError("Should never be reached")


def utf8_bytes_actual_value(num):
    if num < 2**7:
        return [num]
    if num < 2**11:
        return [num & (int("00111111",2) << 6), num & int("00111111",2)]
    if num < 2**16:
        return [(num >>12) & int("00001111", 2) , (num >> 6)  & int("00111111",2), num & int("00111111",2)]
    return [num & (int("00000111", 2) << 15 ), num & (int("00001111", 2) << 11), num & (int("00011111",2) << 6), num & int("00111111",2)]


def utf8_encode(num):
    m = [[]] * 5
    m[2] = [int("11000000", 2), int("10000000", 2)]
    m[3] = [int("11100000", 2), int("10000000", 2), int("10000000", 2)]
    m[4] = [int("11110000", 2) ,int("10000000", 2), int("10000000", 2) , int("10000000", 2)]

    b = utf8_bytes_actual_value(num)
    print("b is ", b)
    if len(b) == 1:
        return b
    return [x | y for x, y in zip(b, m[len(b)])]

def to_hex(nums):
    return " ".join(hex(i) for i in nums)

def utf8_range(lower, upper):
    if utf8_bytes_required(lower) != 2 or utf8_bytes_required(upper) != 2:
        raise RuntimeError("currently only two byte characters supported")
    res = []
    first, second = utf8_encode(lower)
    print("first, second are {} {}".format(hex(first), hex(second)))
    last_second = second
    for i in range(lower + 1, upper + 1):
        cur_first, cur_second = utf8_encode(i)
        if cur_first != first:
            res.append((first, (second, last_second)))
            first = cur_first
            second = cur_second
        last_second = cur_second

    res.append((first, (second, last_second)))
    return res


def to_hex_nested(nested):
    res = []
    for a, (b, c) in nested:
        res.append((hex(a), (hex(b), hex(c))))
    return res


print(to_hex(utf8_encode(int("ff3a", 16))))
print(to_hex_nested(utf8_range(int("c0", 16), int("d6", 16))))

