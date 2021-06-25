import pandas as pd
import psycopg2 as prgrsql

"""from cx import errorcode"""
from src.database.Database import Database


class PostgreSQL(Database):

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
            self.connection = prgrsql.connect(user=self.user, password=self.password, host=self.host,
                                              database='mininclusion')
        except prgrsql.Error as e:
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

    def get_table(self, type_name=None):

        if type_name == '6AfilCto':
            ins_qr = "AfilDiarRGenePorCto6"
        elif type_name == '6AfilCto2':
            ins_qr = "afildiarrgeneporctoclasif6"
        elif type_name == '6AfilCto3':
            ins_qr = 'to_char(T.FECHA_DATO, \'YYYYMM\') as Añomes'
        elif type_name == '5AfilTipo':
            ins_qr = "AfilDiarPorTipo5"
        elif type_name == '5AfilTipo2':
            ins_qr = "afildiartipos5"
        elif type_name == '4AfilDiarReg':
            ins_qr = "AfilDiarGenYAutonPorSeccCNAE4"
        elif type_name == '4AfilDiarReg2':
            ins_qr = "afildiarseccscnae4"
        elif type_name == '31EvolAfilRegims':
            ins_qr = "EvolAfilPorRegim31"
        elif type_name == '32EvolAfilTiposCtos':
            ins_qr = "EvolAfilTipoCtoRegGen32"
        elif type_name == '33AfilDiarRegGenActividades':
            ins_qr = "AfilDiarRegGenPorTipoActiv33"
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
            ins_qr = "RegGenDiarioCNAEProv1"
        elif type_name == '2DIARIOPROVCNAE':
            ins_qr = "RegEspDiarioCNAEProv2"
        return ins_qr

    def get_insqury(self, type_name=None):

        if type_name == '6AfilCto':
            ins_qr = "insert into AfilDiarRGenePorCto6 (FECHA_DATO, IdClasif, Valor) values  "
        elif type_name == '5AfilTipo':
            ins_qr = "insert into AfilDiarPorTipo5 (FECHA_DATO, Idtipo, Valor) values  "
        elif type_name == '4AfilDiarReg':
            ins_qr = "insert into AfilDiarGenYAutonPorSeccCNAE4 (FECHA_DATO, Idregimen, " \
                     "IdsecCNAE, Valor) values "
        elif type_name == '31EvolAfilRegims':
            ins_qr = "insert into EvolAfilPorRegim31 (FECHA_DATO, Idregimen, Valor) values "
        elif type_name == '32EvolAfilTiposCtos':
            ins_qr = "insert into EvolAfilTipoCtoRegGen32 (FECHA_DATO, IdTipoContrato, Valor) values "
        elif type_name == '33AfilDiarRegGenActividades':
            ins_qr = "insert into AfilDiarRegGenPorTipoActiv33 (FECHA_DATO, IdTipoActividad, Valor) values "
        elif type_name == 'Provincia':
            ins_qr = "insert into PrestCNAEProv (FECHA_REPT, IdRD, IdProvincia, COD_CNAE2009, SOLICITUDES, " \
                     "FAVORABLES, DESFAVORABLES, IMPORTE) values "
        elif type_name == 'SerieHistProv0':
            ins_qr = "insert into Serie_Hist_Provincial (IdRD, IdProvincia, FECHA_DATO, " \
                     "Solicitadas, Concedidas, Denegadas) values "
        elif type_name == 'SerieHistProv1':
            ins_qr = "insert into Serie_Hist_Provincial (IdRD, IdProvincia, FECHA_DATO, " \
                     "Solicitadas, Concedidas, Denegadas, Fuerza_Mayor) values "
        elif type_name == '1. SERIE F. NOTIF':
            ins_qr = "insert into ERTE_Serie_F_Notif (FECHA_REPT, FECHA_DATO, Agrup_RDL, " \
                     "Clasificacion, VALOR) values "
        elif type_name == '2. SERIE F.EN ALTA':
            ins_qr = "insert into ERTE_Serie_F_Alta (FECHA_REPT, FECHA_DATO, Agrup_RDL, " \
                     "Clasificacion, VALOR) values "
        elif type_name == '3. DIARIO PROVINCIA':
            ins_qr = "insert into ERTE_Diario_Provincia_Absoluto (FECHA_REPT, FECHA_DATO, IdProvincia, " \
                     "Agrup_RDL, Clasificacion, VALOR) values "
        elif type_name == '4. DIARIO CNAE':
            ins_qr = "insert into ERTE_Diario_CNAE_Absoluto (FECHA_REPT, FECHA_DATO, " \
                     "COD_CNAE2009, Agrup_RDL, Clasificacion, VALOR) values "
        elif type_name == '1DIARIOPROVCNAE':
            ins_qr = "insert into RegGenDiarioCNAEProv1 (FECHA_DATO, IdProvincia, " \
                     "GRP_CNAE2009, COD_CNAE2009, VALOR) values "
        elif type_name == '2DIARIOPROVCNAE':
            ins_qr = "insert into RegEspDiarioCNAEProv2 (FECHA_DATO, IdProvincia, IdRegimen, " \
                     "GRP_CNAE2009, COD_CNAE2009, VALOR) values "
        return ins_qr

    def get_insquryend(self, type_name=None):

        if type_name == '6AfilCto':
            ins_qr = " ON CONFLICT (FECHA_DATO, IdClasif) DO UPDATE SET valor = EXCLUDED.valor ;"
        elif type_name == '5AfilTipo':
            ins_qr = " ON CONFLICT (FECHA_DATO, Idtipo) DO UPDATE SET valor = EXCLUDED.valor ;"
        elif type_name == '4AfilDiarReg':
            ins_qr = " ON CONFLICT (FECHA_DATO, Idregimen, IdsecCNAE) DO UPDATE SET " \
                     "valor = EXCLUDED.valor ;"
        elif type_name == '31EvolAfilRegims':
            ins_qr = " ON CONFLICT (FECHA_DATO, Idregimen) DO UPDATE SET valor = EXCLUDED.valor ;"
        elif type_name == '32EvolAfilTiposCtos':
            ins_qr = " ON CONFLICT (FECHA_DATO, IdTipoContrato) DO UPDATE SET " \
                     "valor = EXCLUDED.valor ;"
        elif type_name == '33AfilDiarRegGenActividades':
            ins_qr = " ON CONFLICT (FECHA_DATO, IdTipoActividad) DO UPDATE SET valor = EXCLUDED.valor;"
        elif type_name == 'Provincia':
            ins_qr = " ON CONFLICT (FECHA_REPT, IdRD, IdProvincia, COD_CNAE2009) DO UPDATE SET valor = EXCLUDED.valor;"
        elif type_name == 'SerieHistProv0':
            ins_qr = " ON CONFLICT (IdRD, IdProvincia, FECHA_DATO) DO UPDATE SET valor = EXCLUDED.valor;"
        elif type_name == 'SerieHistProv1':
            ins_qr = " ON CONFLICT (IdRD, IdProvincia, FECHA_DATO) DO UPDATE SET valor = EXCLUDED.valor;"
        elif type_name == '1. SERIE F. NOTIF':
            ins_qr = " ON CONFLICT (FECHA_REPT, FECHA_DATO, Agrup_RDL, Clasificacion) DO UPDATE SET " \
                     "valor = EXCLUDED.valor ;"
        elif type_name == '2. SERIE F.EN ALTA':
            ins_qr = " ON CONFLICT (FECHA_REPT, FECHA_DATO, Agrup_RDL, Clasificacion) DO UPDATE SET " \
                     "valor = EXCLUDED.valor ;"
        elif type_name == '3. DIARIO PROVINCIA':
            ins_qr = " ON CONFLICT (FECHA_REPT, FECHA_DATO, IdProvincia, Agrup_RDL, Clasificacion) DO UPDATE " \
                     "SET valor = EXCLUDED.valor ;"
        elif type_name == '4. DIARIO CNAE':
            ins_qr = " ON CONFLICT (FECHA_REPT, FECHA_DATO, COD_CNAE2009, Agrup_RDL, Clasificacion) DO UPDATE " \
                     "SET valor = EXCLUDED.valor ;"
        elif type_name == '1DIARIOPROVCNAE':
            ins_qr = " ON CONFLICT (FECHA_DATO, IdProvincia, COD_CNAE2009) DO UPDATE " \
                     " SET valor = EXCLUDED.valor ;"
        elif type_name == '2DIARIOPROVCNAE':
            ins_qr = " ON CONFLICT (FECHA_DATO, IdProvincia, IdRegimen, COD_CNAE2009) " \
                     " DO UPDATE SET valor = EXCLUDED.valor ;"
        return ins_qr

    def get_id(self, type_name=None, code_name=None):
        """
        A method that extracts the ID of a certain benchmark name, given by
        input: either a Derivative, a Credit Spread Curve or an Interest Rate
        Curve
        """
        cursor = self.connection.cursor()

        if type_name == '6AfilCto':
            query = 'SELECT IdClasif FROM AfilDiarRGenePorCtoClasif6 ' \
                    'WHERE \'' + code_name + '\' like \'%\'||Clasificacion||\'%\''
            insert_query = 'insert into AfilDiarRGenePorCtoClasif6 (Clasificacion) values (\'' + code_name + '\')'
        elif type_name == '5AfilTipo':
            query = 'SELECT Idtipo FROM AfilDiarTipos5 WHERE \'' + code_name + '\' like \'%\'||Tipo||\'%\''
            insert_query = 'insert into AfilDiarTipos5 (Tipo) values (\'' + code_name + '\')'
        elif type_name == '4AfilDiarReg':
            query = 'SELECT IdsecCNAE FROM AfilDiarSeccsCNAE4 ' \
                    'WHERE \'' + code_name + '\' like \'%\'||SeccionCNAE||\'%\''
            insert_query = 'insert into AfilDiarSeccsCNAE4 (SeccionCNAE) ' \
                           'values (\'' + code_name + '\')'
        elif type_name == '31EvolAfilRegims':
            query = 'SELECT Idregimen FROM EvolAfilRegims31 ' \
                    'WHERE \'' + code_name + '\' like \'%\'||Regimen||\'%\''
            insert_query = 'insert into EvolAfilRegims31 (Regimen) ' \
                           'values (\'' + code_name + '\')'
        elif type_name == '32EvolAfilTiposCtos':
            query = 'SELECT IdTipoContrato FROM EvolAfilTiposCtos32 ' \
                    'WHERE \'' + code_name + '\' like \'%\'||TipoContrato||\'%\''
            insert_query = 'insert into EvolAfilTiposCtos32 (TipoContrato) ' \
                           'values (\'' + code_name + '\')'
        elif type_name == '33AfilDiarRegGenActividades':
            query = 'SELECT IdTipoActividad FROM AfilDiarRegGenActividades33 ' \
                    'WHERE \'' + code_name + '\' like \'%\'||TipoActividad||\'%\''
            insert_query = 'insert into AfilDiarRegGenActividades33 (TipoActividad) ' \
                           'values (\'' + code_name + '\')'
        elif type_name == 'Provincia':
            query = 'SELECT IdProvincia FROM Provincias ' \
                    'WHERE \'' + code_name + '\' like \'%\'||ProAcron||\'%\''
        elif type_name == 'Provincia2':
            query = 'SELECT IdProvincia FROM Provincias ' \
                    'WHERE \'' + code_name + '\' = IdProvincia'
        elif type_name == 'CNAE':
            query = 'SELECT COD_CNAE2009 FROM CNAE2009 ' \
                    'WHERE \'' + code_name + '\' like \'%\'||COD_CNAE2009||\'%\''

        cursor.execute(query)
        output = cursor.fetchall()
        if cursor.rowcount == 0 and type_name != 'CNAE' and 'Provincia' not in type_name:
            cursor_ins = self.connection.cursor()
            cursor_ins.execute(insert_query)
            self.commit()
            cursor_ins.close()
            cursor.execute(query)
            output = cursor.fetchall()
        elif cursor.rowcount == 0 and type_name == 'CNAE':
            return '00'
        elif cursor.rowcount == 0 and 'Provincia' in type_name:
            return '00'
        cursor.close()
        return output[0][0]

    def get_rdl(self, code_name, ipath):

        cursor = self.connection.cursor()
        query = 'SELECT IdRD FROM Reales_Decretos ' \
                'WHERE \'' + code_name + '\' like \'%\'||RD_largo||\'%\''
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
                       'WHERE IdRD = ' + str(IdRD) + ' and TIPO_CESE = \'' + tipo_cese + '\''
            print(query_tp)
            cursor_tp.execute(query_tp)
            output = cursor_tp.fetchall()
            if cursor_tp.rowcount == 0:
                insert_query = 'insert into Tipos_Cese (IdRD, TIPO_CESE) ' \
                               'values (' + str(IdRD) + ', \'' + str(tipo_cese) + '\')'
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
                'WHERE \'' + code_name + '\' like \'%\'||RD_corto||\'%\''
        cursor.execute(query)
        output = cursor.fetchall()
        if cursor.rowcount == 0:
            rdl = code_name.replace("Rd", "Real Decreto")  # cambiar Real Decreto por Rd
            insert_query = 'insert into Reales_Decretos (RD_corto, RD_largo) ' \
                           'values (\'' + code_name + '\', \'' + rdl + '\')'
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

    def create_view(self, query, view):
        """
        A method designed to execute any query with any parameters
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        cursor.close()
        return view + " creada"

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
