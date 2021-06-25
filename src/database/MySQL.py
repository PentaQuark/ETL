import mysql.connector as cx
import pandas as pd

"""from cx import errorcode"""
from src.database.Database import Database


class MySQL(Database):

    def __init__(self, host, user, password):
        connection = None
        """
        host is localhost
        user is etlUser 
        password is k9Fx3EzQ1Pn4zx$ 
        """
        self.host = host
        self.user = user
        self.password = password
        try:
            self.connection = cx.connect(user=self.user, password=self.password, host=self.host,
                                         database='minInclusion')
        except cx.Error as e:
            print("Error {}:".format(e.args[0]))

    def exec(self, query, swap_name=None, obs_date=None):
        """
        A method to execute any of the necessary queries that extract the data
        needed for the mathematical calculations
        """
        cursor = self.connection.cursor()
        cursor.execute(query, (swap_name, obs_date))
        output = cursor.fetchall()
        cursor.close()
        return output

    def get_id(self, type_name=None, code_name=None):
        """
        A method that extracts the ID of a certain benchmark name, given by
        input: either a Derivative, a Credit Spread Curve or an Interest Rate
        Curve
        """
        cursor = self.connection.cursor()

        if type_name == '6AfilCto':
            query = 'SELECT IdClasif FROM 6AfilDiarRGenePorCtoClasif ' \
                    'WHERE "' + code_name + '" like concat("%", Clasificacion, "%")'
            insert_query = "insert into 6AfilDiarRGenePorCtoClasif (Clasificacion) " \
                           "values ('" + code_name + "')"
        elif type_name == '5AfilTipo':
            query = 'SELECT Idtipo FROM 5AfilDiarTipos ' \
                    'WHERE "' + code_name + '" like concat("%", Tipo, "%")'
            insert_query = "insert into 5AfilDiarTipos (Tipo) " \
                           "values ('" + code_name + "')"
        elif type_name == '4AfilDiarReg':
            query = 'SELECT IdsecCNAE FROM 4AfilDiarSeccsCNAE ' \
                    'WHERE "' + code_name + '" like concat("%", SeccionCNAE, "%")'
            insert_query = "insert into 4AfilDiarSeccsCNAE (SeccionCNAE) " \
                           "values ('" + code_name + "')"
        elif type_name == '31EvolAfilRegims':
            query = 'SELECT Idregimen FROM 31EvolAfilRegims ' \
                    'WHERE "' + code_name + '" like concat("%", Regimen, "%")'
            insert_query = "insert into 31EvolAfilRegims (Regimen) " \
                           "values ('" + code_name + "')"
        elif type_name == '32EvolAfilTiposCtos':
            query = 'SELECT IdTipoContrato FROM 32EvolAfilTiposCtos ' \
                    'WHERE "' + code_name + '" like concat("%", TipoContrato, "%")'
            insert_query = "insert into 32EvolAfilTiposCtos (TipoContrato) " \
                           "values ('" + code_name + "')"
        elif type_name == '33AfilDiarRegGenActividades':
            query = 'SELECT IdTipoActividad FROM 33AfilDiarRegGenActividades ' \
                    'WHERE "' + code_name + '" like concat("%", TipoActividad, "%")'
            insert_query = "insert into 33AfilDiarRegGenActividades (TipoActividad) " \
                           "values ('" + code_name + "')"
        elif type_name == 'Provincia':
            query = 'SELECT IFNULL( (SELECT IdProvincia FROM Provincias ' \
                    'WHERE "' + code_name + '" like concat("%", ProAcron, "%")), "00")'
        elif type_name == 'Provincia2':
            query = 'SELECT IFNULL( (SELECT IdProvincia FROM Provincias ' \
                    'WHERE "' + code_name + '" = IdProvincia), "00")'
        elif type_name == 'CNAE':
            query = 'SELECT IFNULL( (SELECT COD_CNAE2009 FROM CNAE2009 ' \
                    'WHERE "' + code_name + '" like concat("%", COD_CNAE2009, "%")), "00")'

        cursor.execute(query)
        output = cursor.fetchall()
        if cursor.rowcount == 0 and type_name != 'Provincia':
            cursor_ins = self.connection.cursor()
            cursor_ins.execute(insert_query)
            self.commit()
            cursor_ins.close()
            cursor.execute(query)
            output = cursor.fetchall()
        cursor.close()
        return output[0][0]

    def get_table(self, type_name=None):

        if type_name == '6AfilCto':
            ins_qr = "6AfilDiarRGenePorCto"
        elif type_name == '6AfilCto2':
            ins_qr = "6afildiarrgeneporctoclasif"
        elif type_name == '6AfilCto3':
            ins_qr = 'date_format(T.FECHA_DATO, \'YYYYMM\') as Añomes'
        elif type_name == '5AfilTipo':
            ins_qr = "5AfilDiarPorTipo"
        elif type_name == '5AfilTipo2':
            ins_qr = "5afildiartipos"
        elif type_name == '4AfilDiarReg':
            ins_qr = "4AfilDiarGenYAutonPorSeccCNAE"
        elif type_name == '4AfilDiarReg2':
            ins_qr = "afildiarseccscnae4"
        elif type_name == '31EvolAfilRegims':
            ins_qr = "31EvolAfilPorRegim"
        elif type_name == '32EvolAfilTiposCtos':
            ins_qr = "32EvolAfilTipoCtoRegGen"
        elif type_name == '33AfilDiarRegGenActividades':
            ins_qr = "33AfilDiarRegGenPorTipoActiv"
        elif type_name == 'Provincia':
            ins_qr = "PrestCNAEProv"
        elif type_name == 'SerieHistProv0':
            ins_qr = "Serie_Hist_Provincial"
        elif type_name == 'SerieHistProv1':
            ins_qr = "Serie_Hist_Provincial"
        elif type_name == '1. SERIE F. NOTIF':
            ins_qr = "ERTE_Serie_F_Notif"
        elif type_name == '2. SERIE F.EN ALTA':
            ins_qr = "ERTE_Serie_F_Alta"
        elif type_name == '3. DIARIO PROVINCIA':
            ins_qr = "ERTE_Diario_Provincia_Absoluto"
        elif type_name == '4. DIARIO CNAE':
            ins_qr = "ERTE_Diario_CNAE_Absoluto"
        elif type_name == '1DIARIOPROVCNAE':
            ins_qr = "1RegGenDiarioCNAEProv"
        elif type_name == '2DIARIOPROVCNAE':
            ins_qr = "2RegEspDiarioCNAEProv"
        return ins_qr

    def get_insqury(self, type_name=None):

        if type_name == '6AfilCto':
            ins_qr = "insert ignore into 6AfilDiarRGenePorCto (FECHA_REPT, FECHA_DATO, IdClasif, Valor) values  "
        elif type_name == '5AfilTipo':
            ins_qr = "insert ignore into 5AfilDiarPorTipo (FECHA_REPT, FECHA_DATO, Idtipo, Valor) values  "
        elif type_name == '4AfilDiarReg':
            ins_qr = "insert ignore into 4AfilDiarGenYAutonPorSeccCNAE (FECHA_REPT, FECHA_DATO, Idregimen, " \
                     "IdsecCNAE, Valor) values "
        elif type_name == '31EvolAfilRegims':
            ins_qr = "insert ignore into 31EvolAfilPorRegim (FECHA_REPT, FECHA_DATO, Idregimen, Valor) values "
        elif type_name == '32EvolAfilTiposCtos':
            ins_qr = "insert ignore into 32EvolAfilTipoCtoRegGen (FECHA_REPT, FECHA_DATO, IdTipoContrato, " \
                     "Valor) values "
        elif type_name == '33AfilDiarRegGenActividades':
            ins_qr = "insert ignore into 33AfilDiarRegGenPorTipoActiv (FECHA_REPT, FECHA_DATO, " \
                     "IdTipoActividad, Valor) values "
        elif type_name == 'Provincia':
            ins_qr = "insert ignore into PrestCNAEProv (FECHA_REPT, IdRD, IdProvincia, COD_CNAE2009, SOLICITUDES, " \
                     "FAVORABLES, DESFAVORABLES, IMPORTE) values "
        elif type_name == 'SerieHistProv0':
            ins_qr = "insert ignore into Serie_Hist_Provincial (FECHA_REPT, IdRD, IdProvincia, FECHA_DATO, " \
                     "Solicitadas, Concedidas, Denegadas) values "
        elif type_name == 'SerieHistProv1':
            ins_qr = "insert ignore into Serie_Hist_Provincial (FECHA_REPT, IdRD, IdProvincia, FECHA_DATO, " \
                     "Solicitadas, Concedidas, Denegadas, Fuerza_Mayor) values "
        elif type_name == '1. SERIE F. NOTIF':
            ins_qr = "insert ignore into ERTE_Serie_F_Notif (FECHA_REPT, FECHA_DATO, Agrup_RDL, " \
                     "Clasificacion, VALOR) values "
        elif type_name == '2. SERIE F.EN ALTA':
            ins_qr = "insert ignore into ERTE_Serie_F_Alta (FECHA_REPT, FECHA_DATO, Agrup_RDL, " \
                     "Clasificacion, VALOR) values "
        elif type_name == '3. DIARIO PROVINCIA':
            ins_qr = "insert ignore into ERTE_Diario_Provincia_Absoluto (FECHA_REPT, FECHA_DATO, IdProvincia, " \
                     "Agrup_RDL, Clasificacion, VALOR) values "
        elif type_name == '4. DIARIO CNAE':
            ins_qr = "insert ignore into ERTE_Diario_CNAE_Absoluto (FECHA_REPT, FECHA_DATO, " \
                     "COD_CNAE2009, Agrup_RDL, Clasificacion, VALOR) values "
        elif type_name == '1DIARIOPROVCNAE':
            ins_qr = "insert ignore into 1RegGenDiarioCNAEProv (FECHA_REPT, FECHA_DATO, IdProvincia, " \
                     "GRP_CNAE2009, COD_CNAE2009, VALOR) values "
        elif type_name == '2DIARIOPROVCNAE':
            ins_qr = "insert into 2RegEspDiarioCNAEProv (FECHA_REPT, FECHA_DATO, IdProvincia, IdRegimen, " \
                     "GRP_CNAE2009, COD_CNAE2009, VALOR) values "
        return ins_qr

    def get_insquryend(self, type_name=None):

        return ";"

    def get_rdl(self, code_name, ipath):

        cursor = self.connection.cursor()
        query = 'SELECT IdRD FROM Reales_Decretos ' \
                'WHERE "' + code_name + '" like concat("%", RD_largo, "%")'
        cursor.execute(query)
        output = cursor.fetchall()
        pesti = pd.read_excel(ipath, code_name)
        tipo_cese = pesti.iloc[2][2]
        if cursor.rowcount == 0:
            rdc = code_name.replace("Real Decreto", "Rd")  # cambiar Real Decreto por Rd
            insert_query = "insert into Reales_Decretos (RD_largo, RD_corto) " \
                           "values ('" + code_name + "', '" + rdc + "')"
            cursor_ins = self.connection.cursor()
            cursor_ins.execute(insert_query)
            self.commit()
            cursor_ins.close()
            cursor.execute(query)
            output = cursor.fetchall()
            IdRD = output[0][0]
            insert_query = "insert into Tipos_Cese (IdRD, TIPO_CESE) " \
                           "values (" + str(IdRD) + ", '" + str(tipo_cese) + "')"
            cursor_ins = self.connection.cursor()
            cursor_ins.execute(insert_query)
            self.commit()
            cursor_ins.close()
        else:
            IdRD = output[0][0]
            cursor_tp = self.connection.cursor()
            query_tp = 'SELECT IdRD FROM Tipos_Cese ' \
                       'WHERE IdRD = ' + str(IdRD) + ' and TIPO_CESE = "' + tipo_cese + '"'
            print(query_tp)
            cursor_tp.execute(query_tp)
            output = cursor_tp.fetchall()
            if cursor_tp.rowcount == 0:
                insert_query = "insert into Tipos_Cese (IdRD, TIPO_CESE) " \
                               "values (" + str(IdRD) + ", '" + str(tipo_cese) + "')"
                cursor_ins = self.connection.cursor()
                cursor_ins.execute(insert_query)
                self.commit()
                cursor_ins.close()
            cursor_tp.close()

        cursor.close()
        return IdRD

    def get_rdl2(self, code_name):

        cursor = self.connection.cursor()
        query = 'SELECT IdRD FROM Reales_Decretos ' \
                'WHERE "' + code_name + '" like concat("%", RD_corto, "%")'
        cursor.execute(query)
        output = cursor.fetchall()
        if cursor.rowcount == 0:
            rdl = code_name.replace("Rd", "Real Decreto")  # cambiar Real Decreto por Rd
            insert_query = "insert into Reales_Decretos (RD_corto, RD_largo) " \
                           "values ('" + code_name + "', '" + rdl + "')"
            cursor_ins = self.connection.cursor()
            cursor_ins.execute(insert_query)
            self.commit()
            cursor_ins.close()
            cursor.execute(query)
            output = cursor.fetchall()
        cursor.close()
        return output[0][0]

    def get_max_ee_date(self):
        """
        A method that extracts the ID of a certain benchmark name, given by
        input: either a Derivative, a Credit Spread Curve or an Interest Rate
        Curve
        """
        cursor = self.connection.cursor()

        query = "SELECT max(assess_date) from rpa_expositions "
        cursor.execute(query, )
        output = cursor.fetchall()
        cursor.close()
        return output[0][0]

    def exec_param(self, query, params=None):
        """
        A method designed to execute any query with any parameters
        """
        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    def insert(self, query, table):
        cursor = self.connection.cursor()
        cursor.execute(''.join(query))
        self.commit()
        cursor.close()
        return table + " table uploaded"

    def show_rows(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchone()
        cursor.close()
        return rows

    def update(self, query, params=None):
        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        cursor.close()
        return "Data updated correctly"

    def commit(self):
        print("Database changes committed")
        return self.connection.commit()

    def close(self):
        print("Database closed successfully.")
        return self.connection.close()
