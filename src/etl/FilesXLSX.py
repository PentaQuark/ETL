from datetime import datetime

import pandas as pd
import numpy as np
import mysql.connector as cx
# Import module to work with the system
import sys
import src.utils.checkif as checkif


def etl_6diariocto(db, path):
    # t = time.time()
    pest = pd.read_excel(path)
    clasifs = []
    clasif_tp = []
    datarow = 0
    insert_query = db.get_insqury('6AfilCto')
    tabla_dt = db.get_table('6AfilCto')
    tabla_tp = db.get_table('6AfilCto2')
    anomes = db.get_table('6AfilCto3')
    insert_indx = 0
    for rowIndex, row in pest.iterrows():
        header = 0
        column_posic = 0
        for columnIndex, value in row.items():
            if str(value).upper() == "FECHA" and column_posic == 0:
                header = 1
                datarow = rowIndex + 1
            if header == 1:
                if column_posic > 1:
                    try:
                        if not pd.isnull(value):
                            der_id = db.get_id(type_name="6AfilCto", code_name=str(value).rstrip())
                            clasifs.append(der_id)
                            clasif_tp.append(str(value).rstrip())
                    except cx.Error as e:
                        print("Error {}:".format(e.args[0]))
            elif datarow > 0:
                if column_posic == 0:
                    fecha_dato = str(value)[0:10]
                elif column_posic > 1:
                    if not pd.isnull(value) and checkif.is_date(fecha_dato) and checkif.is_integer(str(value)):
                        valor = str(value)
                        if insert_indx > 0:
                            insert_query = insert_query + ", "
                        else:
                            insert_indx = insert_indx + 1
                        insert_query = insert_query + "('" + str(fecha_dato) + "', " \
                                       + str(clasifs[column_posic - 2]) + ", " + valor + ")"

            column_posic = column_posic + 1

        if header == 1:
            qurytxt = 'drop view if exists VISTA_6afildiarrgeneporcto ; CREATE VIEW VISTA_6afildiarrgeneporcto ' \
                      'as SELECT T.FECHA_DATO, ' + anomes
            for j in clasif_tp:
                qurytxt = qurytxt + ', SUM("' + j + '") AS "' + j + '"'
            qurytxt = qurytxt + ' FROM ('
            for idx, val in enumerate(clasif_tp):
                if idx > 0:
                    qurytxt = qurytxt + 'UNION '
                qurytxt = qurytxt + 'select A' + str(idx) + '.FECHA_DATO'
                for k in range(len(clasif_tp)):
                    if k != idx:
                        qurytxt = qurytxt + ', 0 as "' + clasif_tp[k] + '"'
                    else:
                        qurytxt = qurytxt + ', A' + str(idx) + '.valor as "' + clasif_tp[k] + '"'
                qurytxt = qurytxt + ' from ' + tabla_dt + ' as A' + str(idx)
                qurytxt = qurytxt + ' inner join ' + tabla_tp + ' as C' + str(idx) + ' '
                qurytxt = qurytxt + 'on A' + str(idx) + '.IdClasif=C' + str(idx) + '.IdClasif  '
                qurytxt = qurytxt + 'and C' + str(idx) + '.clasificacion=\'' + val + '\' '
            qurytxt = qurytxt + ') AS T GROUP BY T.FECHA_DATO ORDER BY T.FECHA_DATO ; '
            qrfile = open(r"resources/queries/query" + tabla_dt + ".txt", "w+")
            qrfile.write(qurytxt)
            qrfile.close()
            print(db.create_view(qurytxt, "VISTA_6afildiarrgeneporcto"))

    if insert_indx > 0:
        insert_query = insert_query + db.get_insquryend('6AfilCto')
        try:
            print(db.insert(insert_query, "6AfilDiarRGenePorCto"))
        except cx.Error as e:
            print("Error {}:".format(e.args[0]))


def etl_5diariotipo(db, path):
    # t = time.time()
    pest = pd.read_excel(path)
    datarow = 0
    insert_query = db.get_insqury('5AfilTipo')
    insert_indx = 0
    clasif_id = []
    clasif_tp = []
    tabla_dt = db.get_table('5AfilTipo')
    tabla_tp = db.get_table('5AfilTipo2')
    for rowIndex, row in pest.iterrows():
        column_posic = 0
        for columnIndex, value in row.items():
            if datarow == 0 and column_posic > 0:
                try:
                    der_id = db.get_id(type_name="5AfilTipo", code_name=str(columnIndex).rstrip())
                    clasif_tp.append(str(columnIndex).rstrip())
                    clasif_id.append(str(der_id).rstrip())
                except cx.Error as e:
                    print("Error {}:".format(e.args[0]))
            if column_posic == 0:
                fecha_dato = str(value)[0:10]
            elif column_posic > 0:
                if not pd.isnull(value) and checkif.is_date(fecha_dato) and checkif.is_integer(str(value)):
                    valor = str(value)
                    if insert_indx > 0:
                        insert_query = insert_query + ", "
                    else:
                        insert_indx = insert_indx + 1
                    insert_query = insert_query + "('" + str(fecha_dato) + "', " \
                                   + clasif_id[column_posic - 1] + ", " + str(valor) + ")"
            column_posic = column_posic + 1

        if datarow == 0:
            qurytxt = 'drop view if exists VISTA_5afildiarportipo ; CREATE VIEW VISTA_5afildiarportipo ' \
                    'as SELECT T.FECHA_DATO'
            for j in clasif_tp:
                qurytxt = qurytxt + ', SUM("' + j + '") AS "' + j + '"'
            qurytxt = qurytxt + ' FROM ('
            for idx, val in enumerate(clasif_tp):
                if idx > 0:
                    qurytxt = qurytxt + 'UNION '
                qurytxt = qurytxt + 'select A' + str(idx) + '.FECHA_DATO'
                for k in range(len(clasif_tp)):
                    if k != idx:
                        qurytxt = qurytxt + ', 0 as "' + clasif_tp[k] + '"'
                    else:
                        qurytxt = qurytxt + ', A' + str(idx) + '.valor as "' + clasif_tp[k] + '"'
                qurytxt = qurytxt + ' from ' + tabla_dt + ' as A' + str(idx) + ' '
                qurytxt = qurytxt + 'inner join ' + tabla_tp + ' as C' + str(idx) + ' '
                qurytxt = qurytxt + 'on A' + str(idx) + '.idtipo=C' + str(idx) + '.idtipo  '
                qurytxt = qurytxt + 'and C' + str(idx) + '.tipo=\'' + val + '\' '
            qurytxt = qurytxt + ') AS T GROUP BY T.FECHA_DATO ORDER BY T.FECHA_DATO '
            qrfile = open(r"resources/queries/query" + tabla_dt + ".txt", "w+")
            qrfile.write(qurytxt)
            qrfile.close()
            print(db.create_view(qurytxt, "VISTA_5afildiarportipo"))

        datarow = datarow + 1

    if insert_indx > 0:
        insert_query = insert_query + db.get_insquryend('5AfilTipo')
        try:
            print(db.insert(insert_query, "5AfilDiarPorTipo"))
        except cx.Error as e:
            print("Error {}:".format(e.args[0]))


def etl_4SeccionRegGen(db, path):
    # t = time.time()
    tabla_dt = db.get_table('4AfilDiarReg')
    tabla_tp = db.get_table('4AfilDiarReg2')
    for i in range(3):
        Idregimen = i + 1
        if Idregimen == 1:
            regmn = "reg_gen"
        elif Idregimen == 2:
            regmn = "autonom"
        else:
            regmn = "total"
        pest = pd.read_excel(path, i)
        insert_query = db.get_insqury('4AfilDiarReg')
        insert_indx = 0
        clasifs = []
        clasif_tp = []
        datarow = 0
        for rowIndex, row in pest.iterrows():
            header = 0
            column_posic = 0
            for columnIndex, value in row.items():
                if str(value).upper() == "FECHA" and column_posic == 0:
                    header = 1
                    datarow = rowIndex + 1
                if header == 1:
                    if column_posic > 0:
                        try:
                            if not pd.isnull(value):
                                der_id = db.get_id(type_name="4AfilDiarReg", code_name=str(value).rstrip())
                                clasifs.append(der_id)
                                clasif_tp.append(str(value).rstrip())
                        except cx.Error as e:
                            print("Error {}:".format(e.args[0]))
                elif datarow > 0:
                    if column_posic == 0:
                        fecha_dato = str(value)[0:10]
                    elif column_posic > 0:
                        if not pd.isnull(value) and checkif.is_date(fecha_dato) and checkif.is_integer(str(value)):
                            valor = str(value)
                            if insert_indx > 0:
                                insert_query = insert_query + ", "
                            else:
                                insert_indx = insert_indx + 1
                            insert_query = insert_query + "('" + str(fecha_dato) + "', " + str(Idregimen) \
                                           + ", " + str(clasifs[column_posic - 1]) + ", " + valor + ")"

                column_posic = column_posic + 1

            if header == 1:
                qurytxt = 'drop view if exists VISTA_4afildiargenyautonporsecccnae_' + regmn + '; CREATE VIEW ' \
                          'VISTA_4afildiargenyautonporsecccnae_' + regmn + ' AS SELECT T.FECHA_DATO '
                for j in clasif_tp:
                    qurytxt = qurytxt + ', SUM("' + j + '") AS "' + j + '"'
                qurytxt = qurytxt + ' FROM ('
                for idx, val in enumerate(clasif_tp):
                    if idx > 0:
                        qurytxt = qurytxt + 'UNION '
                    qurytxt = qurytxt + 'select A' + str(idx) + '.FECHA_DATO'
                    for k in range(len(clasif_tp)):
                        if k != idx:
                            qurytxt = qurytxt + ', 0 as "' + clasif_tp[k] + '"'
                        else:
                            qurytxt = qurytxt + ', A' + str(idx) + '.valor as "' + clasif_tp[k] + '"'
                    qurytxt = qurytxt + ' from ' + tabla_dt + ' as A' + str(idx) + ' '
                    qurytxt = qurytxt + 'inner join ' + tabla_tp + ' as C' + str(idx) + ' '
                    qurytxt = qurytxt + 'on A' + str(idx) + '.idseccnae=C' + str(idx) + '.idseccnae  '
                    qurytxt = qurytxt + 'and A' + str(idx) + '.idregimen=' + str(Idregimen)
                    qurytxt = qurytxt + ' and C' + str(idx) + '.seccioncnae=\'' + val + '\' '
                qurytxt = qurytxt + ') AS T GROUP BY T.FECHA_DATO ORDER BY T.FECHA_DATO '
                qrfile = open(r"resources/queries/query" + tabla_dt + "_" + regmn + ".txt", "w+")
                qrfile.write(qurytxt)
                qrfile.close()
                print(db.create_view(qurytxt, "VISTA_5afildiarportipo"))

        if insert_indx > 0:
            insert_query = insert_query + db.get_insquryend('4AfilDiarReg')
            try:
                print(db.insert(insert_query, "4AfilDiarGenYAutonPorSeccCNAE"))
            except cx.Error as e:
                print("Error {}:".format(e.args[0]))


def etl_3AfilDiar(db, path):
    # t = time.time()
    for i in range(3):
        pest = pd.read_excel(path, i)
        if i == 0:
            insert_query = db.get_insqury('31EvolAfilRegims')
            end_qy = db.get_insquryend('31EvolAfilRegims')
            indxtable = "31EvolAfilRegims"
            datatable = "31EvolAfilPorRegim"
        elif i == 1:
            insert_query = db.get_insqury('32EvolAfilTiposCtos')
            end_qy = db.get_insquryend('32EvolAfilTiposCtos')
            indxtable = "32EvolAfilTiposCtos"
            datatable = "32EvolAfilTipoCtoRegGen"
        else:
            insert_query = db.get_insqury('33AfilDiarRegGenActividades')
            end_qy = db.get_insquryend('33AfilDiarRegGenActividades')
            indxtable = "33AfilDiarRegGenActividades"
            datatable = "33AfilDiarRegGenPorTipoActiv"

        daterow = 0
        firstdatecolumn = 0
        insert_indx = 0
        dates = []
        num_afil = []
        row_posic = 0

        for rowIndex, row in pest.iterrows():
            column_posic = 0
            for columnIndex, value in row.items():
                valor = str(value).rstrip()
                if daterow == 0 and firstdatecolumn == 0 and checkif.is_date(valor):
                    daterow = row_posic
                    firstdatecolumn = column_posic
                    dates.append(valor[0:10])
                elif daterow == row_posic and row_posic != 0 and checkif.is_date(valor):
                    dates.append(valor[0:10])
                elif daterow == row_posic and row_posic != 0:
                    dates.append(dates[len(dates) - 1])
                if daterow > 0 and row_posic == daterow + 1 and column_posic >= firstdatecolumn:
                    if valor == "Número de Afiliados" or pd.isnull(value):
                        num_afil.append("1")
                    else:
                        num_afil.append("0")
                if daterow > 0 and row_posic > daterow + 1:
                    if column_posic == 0:
                        if not pd.isnull(value):
                            der_id = db.get_id(type_name=indxtable, code_name=valor)
                    else:
                        fecha_dato = dates[column_posic - 1]
                        na = num_afil[column_posic - 1]
                        if not pd.isnull(value) and na == '1' and checkif.is_integer(str(value)):
                            if insert_indx > 0:
                                insert_query = insert_query + ", "
                            else:
                                insert_indx = insert_indx + 1
                            insert_query = insert_query + "('" + str(fecha_dato) + "', " \
                                           + str(der_id) + ", " + valor + ")"

                column_posic = column_posic + 1

            row_posic = row_posic + 1

        if insert_indx > 0:
            insert_query = insert_query + end_qy
            try:
                print(db.insert(insert_query, datatable))
            except cx.Error as e:
                print("Error {}:".format(e.args[0]))


def etl_prestCNAEprov(db, path, fecha_reporte):
    # t = time.time()
    xl = pd.ExcelFile(path)
    tabs = xl.sheet_names
    for i in range(len(tabs)):
        insert_query = db.get_insqury('Provincia')
        rdid = db.get_rdl(tabs[i], path)
        pest = xl.parse(tabs[i])
        cnaesrow = 0
        firstdatacolumn = 0
        insert_indx = 0
        cnaes = []

        row_posic = 0
        for rowIndex, row in pest.iterrows():
            column_posic = 0
            provincia = "00"
            for columnIndex, value in row.items():
                valor = str(value).rstrip()
                if (cnaesrow == 0 or cnaesrow == row_posic) and row_posic >= 4:
                    if not pd.isnull(value) and 'TOTAL' not in valor.upper():
                        cnae = db.get_id(type_name="CNAE", code_name=valor)
                        cnae = cnae.rstrip()
                        cnaes.append(cnae)
                        if cnaesrow == 0:
                            cnaesrow = row_posic
                            firstdatacolumn = column_posic
                    elif cnaesrow == row_posic and pd.isnull(value):
                        cnaes.append(cnaes[len(cnaes) - 1])
                elif (row_posic > cnaesrow + 1) and cnaesrow > 0:
                    if column_posic == 1 and not pd.isnull(value) and 'TOTAL' not in valor.upper():
                        provincia = db.get_id(type_name="Provincia2", code_name=valor)
                    elif provincia != "00":
                        if column_posic - firstdatacolumn < len(cnaes) and cnaes[
                            column_posic - firstdatacolumn] != '00':
                            if ((column_posic - firstdatacolumn) % 4) == 0:
                                ins_add = "('" + fecha_reporte + "', " + str(rdid) + ", '" + provincia + "', '"
                                ins_add = ins_add + cnaes[column_posic - firstdatacolumn] + "'"
                                adding = 0
                            if not pd.isnull(value) and (checkif.is_integer(valor)
                                                         or (checkif.is_float(valor) and (
                                            (column_posic - firstdatacolumn) % 4) == 3)):
                                ins_add = ins_add + ", " + valor
                                adding = adding + 1
                            else:
                                ins_add = ins_add + ", 0"
                            if ((column_posic - firstdatacolumn) % 4) == 3:
                                ins_add = ins_add + ")"
                                if adding > 0 and insert_indx > 0:
                                    insert_query = insert_query + ", "
                                elif adding > 0:
                                    insert_indx = insert_indx + 1
                                if adding > 0:
                                    insert_query = insert_query + ins_add
                                    # adding = 0

                column_posic = column_posic + 1
            row_posic = row_posic + 1

        if insert_indx > 0:
            insert_query = insert_query + db.get_insquryend('Provincia')
            try:
                print(db.insert(insert_query, "PrestCNAEProv" + "__" + tabs[i]))
            except cx.Error as e:
                print("Error {}:".format(e.args[0]))


def etl_SerieHistProv(db, path, fecha_reporte):
    # t = time.time()
    xl = pd.ExcelFile(path)
    tabs = xl.sheet_names
    for i in range(len(tabs)):
        insert_query_0 = db.get_insqury('SerieHistProv0')
        end_query_0 = db.get_insquryend('SerieHistProv0')
        insert_query_1 = db.get_insqury('SerieHistProv1')
        end_query_1 = db.get_insquryend('SerieHistProv1')
        rdid = db.get_rdl2(tabs[i])
        pest = xl.parse(tabs[i])
        # print(pest.head())
        list(pest.columns.values)
        cnaesrow = 0
        firstdatacolumn = 0
        insert_indx_0 = 0
        insert_indx_1 = 0
        dates = []
        dates_sng = {}
        num_type = []

        row_posic = 0
        for rowIndex, row in pest.iterrows():
            column_posic = 0
            dates_ins = {}
            dates_add = {}
            for columnIndex, value in row.items():
                valor = str(value).rstrip()
                if row_posic == 0:
                    if checkif.is_date(str(columnIndex).rstrip()):
                        if firstdatacolumn == 0:
                            firstdatacolumn = column_posic
                        dates.append(str(columnIndex).rstrip()[0:10])
                        dates_sng[str(columnIndex).rstrip()[0:10]] = '0'
                    elif firstdatacolumn > 0:
                        dates.append(dates[len(dates) - 1])
                    if firstdatacolumn > 0 and not pd.isnull(value):
                        num_type.append(valor)
                        if "FUERZA MAYOR" in valor.upper():
                            dates_sng[str(columnIndex).rstrip()[0:10]] = '1'
                    elif firstdatacolumn > 0:
                        num_type.append("_NULL_")
                else:
                    if column_posic == firstdatacolumn - 3:
                        id_prov = db.get_id(type_name="Provincia", code_name=valor)
                    elif column_posic >= firstdatacolumn:
                        if num_type[column_posic - firstdatacolumn].upper() == 'SOLICITADAS':
                            dates_add[dates[column_posic - firstdatacolumn]] = 0
                            if checkif.is_integer(valor) and id_prov != '00':
                                solicitadas = valor
                                dates_add[dates[column_posic - firstdatacolumn]] = dates_add[dates[column_posic \
                                                                                                   - firstdatacolumn]] \
                                                                                   + 1
                            else:
                                solicitadas = "0"
                            dates_ins[dates[column_posic - firstdatacolumn]] = "('" + fecha_reporte + "', " + str(rdid) \
                                                                               + ", '" + id_prov + "', '" \
                                                                               + dates[
                                                                                   column_posic - firstdatacolumn] + \
                                                                               "', " + solicitadas + ", "
                        elif num_type[column_posic - firstdatacolumn].upper() == 'CONCEDIDAS':
                            if checkif.is_integer(valor) and id_prov != '00':
                                concedidas = valor
                                dates_add[dates[column_posic - firstdatacolumn]] = dates_add[dates[column_posic \
                                                                                                   - firstdatacolumn]] \
                                                                                   + 1
                            else:
                                concedidas = "0"
                            dates_ins[dates[column_posic - firstdatacolumn]] = dates_ins[dates[column_posic - \
                                                                                               firstdatacolumn]] + \
                                                                               concedidas + ", "
                        elif num_type[column_posic - firstdatacolumn].upper() == 'DENEGADAS':
                            if checkif.is_integer(valor) and id_prov != '00':
                                denegadas = valor
                                dates_add[dates[column_posic - firstdatacolumn]] = dates_add[dates[column_posic \
                                                                                                   - firstdatacolumn]] \
                                                                                   + 1
                            else:
                                denegadas = "0"
                            dates_ins[dates[column_posic - firstdatacolumn]] = dates_ins[dates[column_posic - \
                                                                                               firstdatacolumn]] + \
                                                                               denegadas
                        elif "FUERZA MAYOR" in num_type[column_posic - firstdatacolumn].upper():
                            if checkif.is_integer(valor) and id_prov != '00':
                                fm = valor
                                dates_add[dates[column_posic - firstdatacolumn]] = dates_add[dates[column_posic \
                                                                                                   - firstdatacolumn]] \
                                                                                   + 1
                            else:
                                fm = "0"
                            dates_ins[dates[column_posic - firstdatacolumn]] = dates_ins[dates[column_posic - \
                                                                                               firstdatacolumn]] \
                                                                               + ", " + fm

                column_posic = column_posic + 1

            if row_posic > 0:
                for key, value in dates_sng.items():
                    dates_ins[key] = dates_ins[key] + ")"
                    if value == '0':
                        if insert_indx_0 > 0 and dates_add[key] > 0:
                            insert_query_0 = insert_query_0 + ", "
                        elif dates_add[key] > 0:
                            insert_indx_0 = insert_indx_0 + 1
                        if dates_add[key] > 0:
                            insert_query_0 = insert_query_0 + dates_ins[key]
                    else:
                        if insert_indx_1 > 0 and dates_add[key] > 0:
                            insert_query_1 = insert_query_1 + ", "
                        elif dates_add[key] > 0:
                            insert_indx_1 = insert_indx_1 + 1
                        if dates_add[key] > 0:
                            insert_query_1 = insert_query_1 + dates_ins[key]

            row_posic = row_posic + 1

        if insert_indx_0 > 0:
            insert_query_0 = insert_query_0 + end_query_0
            try:
                print(db.insert(insert_query_0, "PrestCNAEProv" + "__" + tabs[i]))
            except cx.Error as e:
                print("Error {}:".format(e.args[0]))

        if insert_indx_1 > 0:
            insert_query_1 = insert_query_1 + end_query_1
            try:
                print(db.insert(insert_query_1, "PrestCNAEProv" + "__" + tabs[i]))
            except cx.Error as e:
                print("Error {}:".format(e.args[0]))


def etl_ERTEdiario(db, path, fecha_reporte):
    xl = pd.ExcelFile(path)
    tabs = xl.sheet_names

    for i in range(4):

        if i == 0:
            tab = '1. SERIE F. NOTIF'
            insert_query = db.get_insqury('1. SERIE F. NOTIF')
            end_qry = db.get_insquryend('1. SERIE F. NOTIF')
            tabla = "ERTE_Serie_F_Notif"
        elif i == 1:
            tab = '2. SERIE F.EN ALTA'
            insert_query = db.get_insqury('2. SERIE F.EN ALTA')
            end_qry = db.get_insquryend('2. SERIE F.EN ALTA')
            tabla = "ERTE_Serie_F_Alta"
        elif i == 2:
            tab = '3. DIARIO PROVINCIA'
            insert_query = db.get_insqury('3. DIARIO PROVINCIA')
            end_qry = db.get_insquryend('3. DIARIO PROVINCIA')
            tabla = "ERTE_Diario_Provincia_Absoluto"
        else:
            tab = '4. DIARIO CNAE'
            insert_query = db.get_insqury('4. DIARIO CNAE')
            end_qry = db.get_insquryend('4. DIARIO CNAE')
            tabla = "ERTE_Diario_CNAE_Absoluto"

        pest = xl.parse(tab)

        Agrup_RDL = []
        Clasificacion = []
        insert_indx = 0

        if i == 0:

            row_posic = 0
            for rowIndex, row in pest.iterrows():
                column_posic = 0
                for columnIndex, value in row.items():
                    valor = str(value).rstrip()
                    if row_posic == 1:
                        if column_posic > 0:
                            if not pd.isnull(value):
                                Agrup_RDL.append(valor.upper())
                            else:
                                Agrup_RDL.append(Agrup_RDL[len(Agrup_RDL) - 1])
                    elif row_posic == 2:
                        if column_posic > 0:
                            if not pd.isnull(value):
                                Clasificacion.append(valor.upper())
                            else:
                                if Agrup_RDL[column_posic - 1] == 'TOTAL TODAS LAS CLAVES':
                                    Clasificacion.append('TOTAL TODAS LAS CLAVES')
                                else:
                                    Clasificacion.append(Clasificacion[len(Clasificacion) - 1])
                    elif row_posic > 0:
                        if column_posic == 0:
                            fecha = valor
                        else:
                            if checkif.is_date(fecha) and not pd.isnull(value) and checkif.is_integer(valor):
                                ins_add = "('" + fecha_reporte + "', '" + fecha[0:10] + "', '" + Agrup_RDL[
                                    column_posic] + \
                                          "', '" + Clasificacion[column_posic] + "', " + valor + ")"
                                if insert_indx == 0:
                                    insert_indx = insert_indx + 1
                                else:
                                    insert_query = insert_query + ", "
                                insert_query = insert_query + ins_add

                    column_posic = column_posic + 1
                row_posic = row_posic + 1

        elif i == 1:

            row_posic = 0
            for rowIndex, row in pest.iterrows():
                column_posic = 0
                for columnIndex, value in row.items():
                    valor = str(value).rstrip()
                    if row_posic == 2:
                        if column_posic > 0:
                            if not pd.isnull(value):
                                Agrup_RDL.append(valor.upper())
                            else:
                                Agrup_RDL.append(Agrup_RDL[len(Agrup_RDL) - 1])
                    elif row_posic == 3:
                        if column_posic > 0:
                            if not pd.isnull(value):
                                Clasificacion.append(valor.upper())
                            else:
                                Clasificacion.append(Agrup_RDL[column_posic - 1])
                    elif row_posic == 4:
                        if column_posic > 0:
                            if not pd.isnull(value):
                                Clasificacion[column_posic - 1] = valor
                    elif row_posic > 4:
                        if column_posic == 0:
                            fecha = valor
                        else:
                            if checkif.is_date(fecha) and not pd.isnull(value) and checkif.is_integer(valor):
                                ins_add = "('" + fecha_reporte + "', '" + fecha[0:10] + "', '" + \
                                          Agrup_RDL[column_posic] + "', '" + Clasificacion[column_posic] + \
                                          "', " + valor + ")"
                                if insert_indx == 0:
                                    insert_indx = insert_indx + 1
                                else:
                                    insert_query = insert_query + ", "
                                insert_query = insert_query + ins_add

                    column_posic = column_posic + 1
                row_posic = row_posic + 1

        elif i == 2:

            row_posic = 0
            for rowIndex, row in pest.iterrows():
                column_posic = 0
                for columnIndex, value in row.items():
                    valor = str(value).rstrip()
                    if row_posic == 0:
                        if column_posic == 0:
                            fecha = valor[7:]
                            fecha_tr = fecha.split(' ')
                            if fecha_tr[1].upper() == "ENERO":
                                mes = '01'
                            elif fecha_tr[1].upper() == "FEBRERO":
                                mes = '02'
                            elif fecha_tr[1].upper() == "MARZO":
                                mes = '03'
                            elif fecha_tr[1].upper() == "ABRIL":
                                mes = '04'
                            elif fecha_tr[1].upper() == "MAYO":
                                mes = '05'
                            elif fecha_tr[1].upper() == "JUNIO":
                                mes = '06'
                            elif fecha_tr[1].upper() == "JULIO":
                                mes = '07'
                            elif fecha_tr[1].upper() == "AGOSTO":
                                mes = '08'
                            elif fecha_tr[1].upper() == "SEPTIEMBRE":
                                mes = '09'
                            elif fecha_tr[1].upper() == "OCTUBRE":
                                mes = '10'
                            elif fecha_tr[1].upper() == "NOVIEMBRE":
                                mes = '11'
                            elif fecha_tr[1].upper() == "DICIEMBRE":
                                mes = '12'
                            fecha = fecha_tr[2] + "-" + mes + "-" + fecha_tr[0]
                            if not checkif.is_date(fecha):
                                fecha = ""
                    elif row_posic == 1:
                        if column_posic > 1:
                            if not pd.isnull(value):
                                Agrup_RDL.append(valor)
                            else:
                                Agrup_RDL.append(Agrup_RDL[len(Agrup_RDL) - 1])
                    elif row_posic == 2:
                        if column_posic > 1:
                            if not pd.isnull(value):
                                Clasificacion.append(valor)
                            else:
                                Clasificacion.append(Agrup_RDL[column_posic - 1])
                    elif row_posic > 3:
                        if column_posic == 0:
                            IdProvincia = ''
                            if checkif.is_integer(valor):
                                if len(valor) < 2:
                                    IdProvincia = '0' + valor
                                else:
                                    IdProvincia = valor
                        elif column_posic > 1:
                            if fecha != "" and IdProvincia != '' and not pd.isnull(value) and \
                                    (('%' not in Agrup_RDL[column_posic - 1] and checkif.is_integer(valor))
                                     or ('%' in Agrup_RDL[column_posic - 1] and checkif.is_float(valor))):
                                ins_add = "('" + fecha_reporte + "', '" + fecha + "', '" + \
                                          IdProvincia + "', '" + Agrup_RDL[column_posic - 1] + "', '" + \
                                          Clasificacion[column_posic - 1] + "', " + valor + ")"
                                if insert_indx == 0:
                                    insert_indx = insert_indx + 1
                                else:
                                    insert_query = insert_query + ", "
                                insert_query = insert_query + ins_add

                    column_posic = column_posic + 1
                row_posic = row_posic + 1

        elif i == 3:

            row_posic = 0
            for rowIndex, row in pest.iterrows():
                column_posic = 0
                for columnIndex, value in row.items():
                    valor = str(value).rstrip()
                    if row_posic == 0:
                        if column_posic == 0:
                            fecha = valor[7:]
                            fecha_tr = fecha.split(' ')
                            if fecha_tr[1].upper() == "ENERO":
                                mes = '01'
                            elif fecha_tr[1].upper() == "FEBRERO":
                                mes = '02'
                            elif fecha_tr[1].upper() == "MARZO":
                                mes = '03'
                            elif fecha_tr[1].upper() == "ABRIL":
                                mes = '04'
                            elif fecha_tr[1].upper() == "MAYO":
                                mes = '05'
                            elif fecha_tr[1].upper() == "JUNIO":
                                mes = '06'
                            elif fecha_tr[1].upper() == "JULIO":
                                mes = '07'
                            elif fecha_tr[1].upper() == "AGOSTO":
                                mes = '08'
                            elif fecha_tr[1].upper() == "SEPTIEMBRE":
                                mes = '09'
                            elif fecha_tr[1].upper() == "OCTUBRE":
                                mes = '10'
                            elif fecha_tr[1].upper() == "NOVIEMBRE":
                                mes = '11'
                            elif fecha_tr[1].upper() == "DICIEMBRE":
                                mes = '12'
                            fecha = fecha_tr[2] + "-" + mes + "-" + fecha_tr[0]
                            if not checkif.is_date(fecha):
                                fecha = ""
                    elif row_posic == 1:
                        if column_posic > 1:
                            if not pd.isnull(value):
                                Agrup_RDL.append(valor)
                            else:
                                Agrup_RDL.append(Agrup_RDL[len(Agrup_RDL) - 1])
                    elif row_posic == 2:
                        if column_posic > 1:
                            if not pd.isnull(value):
                                Clasificacion.append(valor)
                            else:
                                Clasificacion.append(Agrup_RDL[column_posic - 1])
                    elif row_posic > 3:
                        if column_posic == 0:
                            IdCNAE = ''
                            if checkif.is_integer(valor):
                                IdCNAE = valor
                        elif column_posic > 1:
                            if fecha != "" and IdCNAE != '' and not pd.isnull(value) and \
                                    (('%' not in Agrup_RDL[column_posic - 1] and checkif.is_integer(valor))
                                     or ('%' in Agrup_RDL[column_posic - 1] and checkif.is_float(valor))):
                                ins_add = "('" + fecha_reporte + "', '" + fecha + "', '" + \
                                          IdCNAE + "', '" + Agrup_RDL[column_posic - 1] + "', '" + \
                                          Clasificacion[column_posic - 1] + "', " + valor + ")"
                                if insert_indx == 0:
                                    insert_indx = insert_indx + 1
                                else:
                                    insert_query = insert_query + ", "
                                insert_query = insert_query + ins_add

                    column_posic = column_posic + 1
                row_posic = row_posic + 1

        if insert_indx > 0:
            insert_query = insert_query + end_qry
            try:
                print(db.insert(insert_query, tabla))
            except cx.Error as e:
                print("Error {}:".format(e.args[0]))


def etl_1diarioreggen(db, path):
    insert_query = db.get_insqury('1DIARIOPROVCNAE')
    tabla_dt = db.get_table('1DIARIOPROVCNAE')
    end_query = db.get_insquryend('1DIARIOPROVCNAE')
    t = datetime.now()
    print("0_" + t.strftime("%H:%M:%S"))
    pest = pd.read_excel(path, 2)
    pest.iloc[:, 0] = pest.iloc[:, 0].map(str)
    pest.iloc[:, 0] = pest.apply(lambda x: checkif.is_10_date(x.iloc[0]), axis=1)
    pest = pest[pest.iloc[:, 0] != '0000-00-00']
    pest.iloc[:, 3] = pest.iloc[:, 3].map(str)
    pest.iloc[:, 3] = pest.apply(lambda x: checkif.is_two_integer(x.iloc[3]), axis=1)
    pest = pest[pest.iloc[:, 3] != '00']
    pest.iloc[:, 5] = pest.iloc[:, 5].map(str)
    pest.iloc[:, 5] = pest.apply(lambda x: checkif.is_grp_cnae2009(x.iloc[5]), axis=1)
    pest.iloc[:, 7] = pest.iloc[:, 7].map(str)
    pest.iloc[:, 7] = pest.apply(lambda x: checkif.is_cod_cnae2009(x.iloc[7]), axis=1)
    pest.iloc[:, 9] = pest.iloc[:, 9].map(str)
    pest.iloc[:, 9] = pest.apply(lambda x: checkif.is_int_value(x.iloc[9]), axis=1)
    pest = pest[pest.iloc[:, 9] != '0']
    insertsql2 = "('" + pest.iloc[:, 0] + "', '" + pest.iloc[:, 3] + "', '" + pest.iloc[:, 5] \
                 + "', '" + pest.iloc[:, 7] + "', " + pest.iloc[:, 9] + ")"
    insertsql2.iloc[0] = insert_query + insertsql2.iloc[0]
    insertsql2.iloc[:-1] = insertsql2.iloc[:-1] + ", "
    insertsql2.iloc[-1] = insertsql2.iloc[-1] + end_query
    np.savetxt(r"resources/queries/insert_into_" + tabla_dt + ".txt", insertsql2.values, fmt='%s')
    sqlfile = open("resources/queries/insert_into_" + tabla_dt + ".txt")
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
    pest = pd.read_excel(path, 3)
    pest.iloc[:, 0] = pest.iloc[:, 0].map(str)
    pest.iloc[:, 0] = pest.apply(lambda x: checkif.is_10_date(x.iloc[0]), axis=1)
    pest = pest[pest.iloc[:, 0] != '0000-00-00']
    pest.iloc[:, 3] = pest.iloc[:, 3].map(str)
    pest.iloc[:, 3] = pest.apply(lambda x: checkif.is_two_integer(x.iloc[3]), axis=1)
    pest = pest[pest.iloc[:, 3] != '00']
    pest.iloc[:, 5] = pest.iloc[:, 5].map(str)
    pest.iloc[:, 5] = pest.apply(lambda x: checkif.is_grp_cnae2009(x.iloc[5]), axis=1)
    pest.iloc[:, 7] = pest.iloc[:, 7].map(str)
    pest.iloc[:, 7] = pest.apply(lambda x: checkif.is_cod_cnae2009(x.iloc[7]), axis=1)
    pest.iloc[:, 9] = pest.iloc[:, 9].map(str)
    pest.iloc[:, 9] = pest.apply(lambda x: checkif.is_int_value(x.iloc[9]), axis=1)
    pest = pest[pest.iloc[:, 9] != '0']
    insertsql2 = "('" + pest.iloc[:, 0] + "', '" + pest.iloc[:, 3] + "', '" + pest.iloc[:, 5] \
                 + "', '" + pest.iloc[:, 7] + "', " + pest.iloc[:, 9] + ")"
    insertsql2.iloc[0] = insert_query + insertsql2.iloc[0]
    insertsql2.iloc[:-1] = insertsql2.iloc[:-1] + ", "
    insertsql2.iloc[-1] = insertsql2.iloc[-1] + end_query
    np.savetxt(r"resources/queries/insert_into_" + tabla_dt + ".txt", insertsql2.values, fmt='%s')
    sqlfile = open("resources/queries/insert_into_" + tabla_dt + ".txt")
    sqlcommand = sqlfile.read().replace("\n", " ")
    sqlfile.close()
    t = datetime.now()
    print("1_" + t.strftime("%H:%M:%S"))
    try:
        print(db.insert(sqlcommand, "1DIARIOPROVCNAE"))
    except cx.Error as e:
        print("Error {}:".format(e.args[0]))
    t = datetime.now()
    print("1_" + t.strftime("%H:%M:%S"))


def etl_2diarioregesp(db, path):
    insert_query = db.get_insqury('2DIARIOPROVCNAE')
    tabla_dt = db.get_table('2DIARIOPROVCNAE')
    end_query = db.get_insquryend('2DIARIOPROVCNAE')
    t = datetime.now()
    print("0_" + t.strftime("%H:%M:%S"))
    pest = pd.read_excel(path, 2)
    pest.iloc[:, 0] = pest.iloc[:, 0].map(str)
    pest.iloc[:, 0] = pest.apply(lambda x: checkif.is_10_date(x.iloc[0]), axis=1)
    pest = pest[pest.iloc[:, 0] != '0000-00-00']
    pest.iloc[:, 3] = pest.iloc[:, 3].map(str)
    pest.iloc[:, 3] = pest.apply(lambda x: checkif.is_two_integer(x.iloc[3]), axis=1)
    pest = pest[pest.iloc[:, 3] != '00']
    pest.iloc[:, 5] = pest.iloc[:, 5].map(str)
    pest.iloc[:, 5] = pest.apply(lambda x: checkif.is_ok_regimen(x.iloc[5]), axis=1)
    pest = pest[pest.iloc[:, 5] != '0']
    pest.iloc[:, 7] = pest.iloc[:, 7].map(str)
    pest.iloc[:, 7] = pest.apply(lambda x: checkif.is_grp_cnae2009(x.iloc[7]), axis=1)
    pest.iloc[:, 9] = pest.iloc[:, 9].map(str)
    pest.iloc[:, 9] = pest.apply(lambda x: checkif.is_cod_cnae2009(x.iloc[9]), axis=1)
    pest.iloc[:, 11] = pest.iloc[:, 11].map(str)
    pest.iloc[:, 11] = pest.apply(lambda x: checkif.is_int_value(x.iloc[11]), axis=1)
    pest = pest[pest.iloc[:, 11] != '0']
    insertsql2 = "('" + pest.iloc[:, 0] + "', '" + pest.iloc[:, 3] \
                 + "', " + pest.iloc[:, 5] + ", '" + pest.iloc[:, 7] + "', '" + pest.iloc[:, 9] \
                 + "', " + pest.iloc[:, 11] + ")"
    insertsql2.iloc[0] = insert_query + insertsql2.iloc[0]
    insertsql2.iloc[:-1] = insertsql2.iloc[:-1] + ", "
    insertsql2.iloc[-1] = insertsql2.iloc[-1] + end_query
    np.savetxt(r"resources/queries/insert_into_" + tabla_dt + ".txt", insertsql2.values, fmt='%s')
    sqlfile = open("resources/queries/insert_into_" + tabla_dt + ".txt")
    sqlcommand = sqlfile.read().replace("\n", " ")
    sqlfile.close()
    t = datetime.now()
    print("1_" + t.strftime("%H:%M:%S"))
    try:
        print(db.insert(sqlcommand, "2DIARIOPROVCNAE"))
    except cx.Error as e:
        print("Error {}:".format(e.args[0]))
    t = datetime.now()
    print("0_" + t.strftime("%H:%M:%S"))
    pest = pd.read_excel(path, 3)
    pest.iloc[:, 0] = pest.iloc[:, 0].map(str)
    pest.iloc[:, 0] = pest.apply(lambda x: checkif.is_10_date(x.iloc[0]), axis=1)
    pest = pest[pest.iloc[:, 0] != '0000-00-00']
    pest.iloc[:, 3] = pest.iloc[:, 3].map(str)
    pest.iloc[:, 3] = pest.apply(lambda x: checkif.is_two_integer(x.iloc[3]), axis=1)
    pest = pest[pest.iloc[:, 3] != '00']
    pest.iloc[:, 5] = pest.iloc[:, 5].map(str)
    pest.iloc[:, 5] = pest.apply(lambda x: checkif.is_ok_regimen(x.iloc[5]), axis=1)
    pest = pest[pest.iloc[:, 5] != '0']
    pest.iloc[:, 7] = pest.iloc[:, 7].map(str)
    pest.iloc[:, 7] = pest.apply(lambda x: checkif.is_grp_cnae2009(x.iloc[7]), axis=1)
    pest.iloc[:, 9] = pest.iloc[:, 9].map(str)
    pest.iloc[:, 9] = pest.apply(lambda x: checkif.is_cod_cnae2009(x.iloc[9]), axis=1)
    pest.iloc[:, 11] = pest.iloc[:, 11].map(str)
    pest.iloc[:, 11] = pest.apply(lambda x: checkif.is_int_value(x.iloc[11]), axis=1)
    pest = pest[pest.iloc[:, 11] != '0']
    insertsql2 = "('" + pest.iloc[:, 0] + "', '" + pest.iloc[:, 3] \
                 + "', " + pest.iloc[:, 5] + ", '" + pest.iloc[:, 7] + "', '" + pest.iloc[:, 9] \
                 + "', " + pest.iloc[:, 11] + ")"
    insertsql2.iloc[0] = insert_query + insertsql2.iloc[0]
    insertsql2.iloc[:-1] = insertsql2.iloc[:-1] + ", "
    insertsql2.iloc[-1] = insertsql2.iloc[-1] + end_query
    np.savetxt(r"resources/queries/insert_into_" + tabla_dt + ".txt", insertsql2.values, fmt='%s')
    sqlfile = open("resources/queries/insert_into_" + tabla_dt + ".txt")
    sqlcommand = sqlfile.read().replace("\n", " ")
    sqlfile.close()
    t = datetime.now()
    print("1_" + t.strftime("%H:%M:%S"))
    try:
        print(db.insert(sqlcommand, "2DIARIOPROVCNAE"))
    except cx.Error as e:
        print("Error {}:".format(e.args[0]))
    t = datetime.now()
    print(t.strftime("%H:%M:%S"))
