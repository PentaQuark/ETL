from src.database.MySQL import MySQL
from src.database.PostgreSQL import PostgreSQL
import src.etl.FilesXLSX as exclImport

# db = MySQL("localhost", "etlUser", "k9Fx3EzQ1Pn4zx$")
# db = PostgreSQL("localhost", "etlUser", "k9Fx3EzQ1Pn4zx$")
db = PostgreSQL("localhost", "postgres", "p0stgr3s")

inputpath = "/home/demiurgo/Quark/Ministerio/respaldo/"

# exclImport.etl_6diariocto(db, inputpath + "6.DIARIO tipo contrato desde 2009.xlsx", \
#                           "2021-05-28")
# exclImport.etl_5diariotipo(db, inputpath + "5.Datos diarios.xlsx", "2021-05-28")
# exclImport.etl_4SeccionRegGen(db, inputpath + "4. SECCIONES R-GENERAL MAS AUTONOMOS "
#                                   "DIARIO.xlsx", "2021-05-28")

# exclImport.etl_3AfilDiar(db, inputpath + "3.Datos diarios_Afiliados regimenes_TC_Secciones.xlsx", "2021-05-28")

# exclImport.etl_prestCNAEprov(db, inputpath + "Prestaciones por cnae y prov a 29 marzo_Mutuas.xlsx", "2021-05-28")
# exclImport.etl_SerieHistProv(db, inputpath + "serie histórica provincial a 29 marzo_Mutuas.xlsx", "2021-05-28")

# exclImport.etl_ERTEdiario(db, inputpath + "1304_ERTE_FICHERO DIARIO.xlsx", "2021-05-28")

inputpath = "/home/demiurgo/Quark/Ministerio/Entradas/"
exclImport.etl_1diarioreggen(db, inputpath + "1_Provincia_diario_actividad_Cnae09_R_General.xlsx", "2021-05-28")
# exclImport.etl_2diarioregesp(db, inputpath + "2_Provincia_diario_actividad_Cnae09_RETA_SETA-NOSETA.xlsx", "2021-05-28")

# 5.Datos diarios.xlsx


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
