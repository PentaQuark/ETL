from dateutil.parser import parse


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False


def is_integer(string):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    if string.isnumeric() and int(string) != 0:
        return True
    elif string.isnumeric():
        return False
    else:
        try:
            decmal = float(string)
            resto = decmal - int(decmal)
            if resto == 0 and decmal != 0:
                return True
            else:
                return False
        except ValueError:
            return False


def is_float(string):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    if string.isnumeric() and int(string) != 0:
        return True
    elif string.isnumeric():
        return False
    else:
        try:
            decmal = float(string)
            if decmal != 0:
                return True
            else:
                return False
        except ValueError:
            return False


def is_two_integer(string):
    if is_integer(string) and len(string) < 2:
        string = '0' + string
    elif not is_integer(string):
        string = '00'
    return string


def is_10_date(string):
    if not is_date(string):
        string = '0000-00-00'
    else:
        string = string[0:10]
    return string


def is_ok_regimen(string):
    if not is_integer(string) or (int(string) < 1):
        string = '0'
    return string


def is_grp_cnae2009(string):
    cnae_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
                 'U']
    if string not in cnae_list:
        string = 'X'
    return string


def is_cod_cnae2009(string):
    if not is_integer(string) or (9900 < int(string) or int(string) < 1):
        string = 'X'
    return string


def is_int_value(string):
    if not is_integer(string) or int(string) < 0:
        string = '0'
    return string
