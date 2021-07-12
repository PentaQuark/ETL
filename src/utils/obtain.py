import src.utils.checkif as checkif


def grp_cnae2009(string):
    if checkif.is_integer(string):
        if '01' <= string < '05':
            grp_cnae = 'A'
        elif string < '10':
            grp_cnae = 'B'
        elif string < '35':
            grp_cnae = 'C'
        elif string < '36':
            grp_cnae = 'D'
        elif string < '41':
            grp_cnae = 'E'
        elif string < '45':
            grp_cnae = 'F'
        elif string < '49':
            grp_cnae = 'G'
        elif string < '55':
            grp_cnae = 'H'
        elif string < '58':
            grp_cnae = 'I'
        elif string < '64':
            grp_cnae = 'J'
        elif string < '68':
            grp_cnae = 'K'
        elif string < '69':
            grp_cnae = 'L'
        elif string < '77':
            grp_cnae = 'M'
        elif string < '84':
            grp_cnae = 'N'
        elif string < '85':
            grp_cnae = 'O'
        elif string < '86':
            grp_cnae = 'P'
        elif string < '90':
            grp_cnae = 'Q'
        elif string < '94':
            grp_cnae = 'R'
        elif string < '97':
            grp_cnae = 'S'
        elif string < '99':
            grp_cnae = 'T'
        elif string <= '9900':
            grp_cnae = 'U'
        else:
            grp_cnae = 'X'
    else:
        grp_cnae = 'X'
    return grp_cnae


def db_date(string):
    if checkif.is_sp_date(string):
        date = string[6:10] + '-' + string[3:5] + '-' + string[0:2]
    else:
        date = '0000-00-00'
    return date

