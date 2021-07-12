from datetime import datetime

import pandas as pd
import numpy as np
import mysql.connector as cx
# Import module to work with the system
import sys
import src.utils.checkif as checkif
import src.utils.obtain as obtain


def etl_1diarioreggen(db, path):
    insert_query = db.get_insqury('1DIARIOPROVCNAE')
    tabla_dt = db.get_table('1DIARIOPROVCNAE')
    end_query = db.get_insquryend('1DIARIOPROVCNAE')
    t = datetime.now()
    print("0_" + t.strftime("%H:%M:%S"))
    pest = pd.read_csv(path, sep=';', delimiter=None, dtype=object)
    pest.iloc[:, 0] = pest.iloc[:, 0].map(str)
    pest.iloc[:, 0] = pest.apply(lambda x: obtain.db_date(x.iloc[0]), axis=1)
    pest = pest[pest.iloc[:, 0] != '0000-00-00']
    pest.iloc[:, 1] = pest.iloc[:, 1].map(str)
    pest.iloc[:, 1] = pest.apply(lambda x: checkif.is_two_integer(x.iloc[1]), axis=1)
    pest = pest[pest.iloc[:, 1] != '00']
    pest.iloc[:, 3] = pest.iloc[:, 3].map(str)
    pest.iloc[:, 3] = pest.apply(lambda x: checkif.is_cod_cnae2009(x.iloc[3]), axis=1)
    pest.iloc[:, 5] = pest.iloc[:, 5].map(str)
    pest.iloc[:, 5] = pest.apply(lambda x: checkif.is_int_value(x.iloc[5]), axis=1)
    pest = pest[pest.iloc[:, 5] != '0']
    pest['group_cnae'] = pest.apply(lambda x: obtain.grp_cnae2009(x.iloc[3]), axis=1)
    insertsql2 = "('" + pest.iloc[:, 0] + "', '" + pest.iloc[:, 1] + "', '" + pest.iloc[:, 6] \
                 + "', '" + pest.iloc[:, 3] + "', " + pest.iloc[:, 5] + ")"
    insertsql2.iloc[0] = insert_query + insertsql2.iloc[0]
    insertsql2.iloc[:-1] = insertsql2.iloc[:-1] + ", "
    insertsql2.iloc[-1] = insertsql2.iloc[-1] + end_query
    np.savetxt(r"resources/queries/insert_into_" + tabla_dt + "_csv.txt", insertsql2.values, fmt='%s')
    sqlfile = open("resources/queries/insert_into_" + tabla_dt + "_csv.txt")
    sqlcommand = sqlfile.read().replace("\n", " ")
    sqlfile.close()
    t = datetime.now()
    print("1_" + t.strftime("%H:%M:%S"))
    try:
        print(db.insert(sqlcommand, "1DIARIOPROVCNAE"))
    except cx.Error as e:
        print("Error {}:".format(e.args[0]))
    t = datetime.now()
    print("0_" + t.strftime("%H:%M:%S"))


def etl_2diarioreta(db, path):
    insert_query = db.get_insqury('2DIARIOPROVCNAE')
    tabla_dt = db.get_table('2DIARIOPROVCNAE')
    end_query = db.get_insquryend('2DIARIOPROVCNAE')
    t = datetime.now()
    print("0_" + t.strftime("%H:%M:%S"))
    pest = pd.read_csv(path, sep=';', delimiter=None, dtype=object)
    pest.iloc[:, 0] = pest.iloc[:, 0].map(str)
    pest.iloc[:, 0] = pest.apply(lambda x: obtain.db_date(x.iloc[0]), axis=1)
    pest = pest[pest.iloc[:, 0] != '0000-00-00']
    pest.iloc[:, 1] = pest.iloc[:, 1].map(str)
    pest.iloc[:, 1] = pest.apply(lambda x: checkif.is_two_integer(x.iloc[1]), axis=1)
    pest = pest[pest.iloc[:, 1] != '00']
    pest.iloc[:, 3] = pest.iloc[:, 3].map(str)
    pest.iloc[:, 3] = pest.apply(lambda x: checkif.is_cod_cnae2009(x.iloc[3]), axis=1)
    pest.iloc[:, 5] = pest.iloc[:, 5].map(str)
    pest.iloc[:, 5] = pest.apply(lambda x: checkif.is_int_value(x.iloc[5]), axis=1)
    pest = pest[pest.iloc[:, 5] != '0']
    pest['group_cnae'] = pest.apply(lambda x: obtain.grp_cnae2009(x.iloc[3]), axis=1)
    insertsql2 = "('" + pest.iloc[:, 0] + "', '" + pest.iloc[:, 1] + "',521, '" + pest.iloc[:, 6] \
                 + "', '" + pest.iloc[:, 3] + "', " + pest.iloc[:, 5] + ")"
    insertsql2.iloc[0] = insert_query + insertsql2.iloc[0]
    insertsql2.iloc[:-1] = insertsql2.iloc[:-1] + ", "
    insertsql2.iloc[-1] = insertsql2.iloc[-1] + end_query
    np.savetxt(r"resources/queries/insert_into_" + tabla_dt + "_csv.txt", insertsql2.values, fmt='%s')
    sqlfile = open("resources/queries/insert_into_" + tabla_dt + "_csv.txt")
    sqlcommand = sqlfile.read().replace("\n", " ")
    sqlfile.close()
    t = datetime.now()
    print("1_" + t.strftime("%H:%M:%S"))
    try:
        print(db.insert(sqlcommand, "1DIARIOPROVCNAE"))
    except cx.Error as e:
        print("Error {}:".format(e.args[0]))
    t = datetime.now()
    print("0_" + t.strftime("%H:%M:%S"))


def etl_2diarioseta(db, path):
    insert_query = db.get_insqury('2DIARIOPROVCNAE')
    tabla_dt = db.get_table('2DIARIOPROVCNAE')
    end_query = db.get_insquryend('2DIARIOPROVCNAE')
    t = datetime.now()
    print("0_" + t.strftime("%H:%M:%S"))
    pest = pd.read_csv(path, sep=';', delimiter=None, dtype=object)
    pest.iloc[:, 0] = pest.iloc[:, 0].map(str)
    pest.iloc[:, 0] = pest.apply(lambda x: obtain.db_date(x.iloc[0]), axis=1)
    pest = pest[pest.iloc[:, 0] != '0000-00-00']
    pest.iloc[:, 1] = pest.iloc[:, 1].map(str)
    pest.iloc[:, 1] = pest.apply(lambda x: checkif.is_two_integer(x.iloc[1]), axis=1)
    pest = pest[pest.iloc[:, 1] != '00']
    pest.iloc[:, 3] = pest.iloc[:, 3].map(str)
    pest.iloc[:, 3] = pest.apply(lambda x: checkif.is_cod_cnae2009(x.iloc[3]), axis=1)
    pest.iloc[:, 5] = pest.iloc[:, 5].map(str)
    pest.iloc[:, 5] = pest.apply(lambda x: checkif.is_int_value(x.iloc[5]), axis=1)
    pest = pest[pest.iloc[:, 5] != '0']
    pest['group_cnae'] = pest.apply(lambda x: obtain.grp_cnae2009(x.iloc[3]), axis=1)
    insertsql2 = "('" + pest.iloc[:, 0] + "', '" + pest.iloc[:, 1] + "',571, '" + pest.iloc[:, 6] \
                 + "', '" + pest.iloc[:, 3] + "', " + pest.iloc[:, 5] + ")"
    insertsql2.iloc[0] = insert_query + insertsql2.iloc[0]
    insertsql2.iloc[:-1] = insertsql2.iloc[:-1] + ", "
    insertsql2.iloc[-1] = insertsql2.iloc[-1] + end_query
    np.savetxt(r"resources/queries/insert_into_" + tabla_dt + "_csv.txt", insertsql2.values, fmt='%s')
    sqlfile = open("resources/queries/insert_into_" + tabla_dt + "_csv.txt")
    sqlcommand = sqlfile.read().replace("\n", " ")
    sqlfile.close()
    t = datetime.now()
    print("1_" + t.strftime("%H:%M:%S"))
    try:
        print(db.insert(sqlcommand, "1DIARIOPROVCNAE"))
    except cx.Error as e:
        print("Error {}:".format(e.args[0]))
    t = datetime.now()
    print("0_" + t.strftime("%H:%M:%S"))
