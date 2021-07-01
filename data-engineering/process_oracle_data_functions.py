from sqlalchemy import types
from pandas import read_sql_query
from sqlalchemy.sql.sqltypes import Time
import traceback


def tiposdecolumnaportabla(dfparam):
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "int" in str([type(v) for v in dfparam[i].values]):
            dtypedict.update({i: types.INT()})
        if "float" in str([type(v) for v in dfparam[i].values]):
            dtypedict.update({i: types.Float(precision=9, asdecimal=True)})
        if "time" in str([type(v) for v in dfparam[i].values]):
        #if "datetime" in str([type(v) for v in dfparam[i].values]):
            dtypedict.update({i: types.Date()})
            pass
        if "object" in str([type(v) for v in dfparam[i].values]):
            dtypedict.update({i: types.VARCHAR(max([len(str(n)) for n in dfparam[i].values]))})
        if "str" in str([type(v) for v in dfparam[i].values]):
            dtypedict.update({i: types.VARCHAR(max([len(str(n)) for n in dfparam[i].values]))})
    return dtypedict

#Función para crear un query de creación de tabla en oracle
def create_empty_table_from_df(dfparam,esquema,tabla):
    dtypedict = tiposdecolumnaportabla(dfparam)
    #Lo convertimos a string
    str_dtypedict = ', '.join("{!s} {!r}".format(key,val) for (key,val) in dtypedict.items())
    str_dtypedict = str_dtypedict.replace("'","")
    #Query final
    query = f'create table {esquema}.{tabla} ('+str_dtypedict+')'
    return query

#estandarizar nombre de columnas
#limpiar data
def createNewTableWithDF(datosdf,tablename,engine):
    CHUNKZIZE = 100000
    diccionarioColumnas = tiposdecolumnaportabla(datosdf)
    datosdf.to_sql(name=tablename.lower(), con=engine, if_exists='replace', index=False, dtype=diccionarioColumnas, chunksize = CHUNKZIZE)

def appendNewTableWithDF(datosdf,tablename,engine):
    CHUNKZIZE = 100000
    diccionarioColumnas = tiposdecolumnaportabla(datosdf)
    datosdf.to_sql(name=tablename.lower(), con=engine, if_exists='append', index=False, dtype=diccionarioColumnas, chunksize = CHUNKZIZE)

def trimAndRemoveEntersFromVarcharOracleColumn(nombreTabla,esquemaTabla,engine):
    #SET SERVEROUTPUT ON SIZE 100000
    #ALTER SESSION ENABLE PARALLEL DML
    #exec SP_LIMPIA_TABLA('CONOSCE_SNIP','SCH_UNIVERSAL');
    try:
        connection = engine.raw_connection()
        cursor = connection.cursor()
        cursor.callproc("SP_LIMPIA_TABLA", [nombreTabla, esquemaTabla])
        #results = list(cursor.fetchall())
        cursor.close()
        # connection.commit()
    except Exception as e:
        error = e.args
        print(error)

def cleanEntersAndLinereturnsFromPandas(datosdf):
    datosdf = datosdf.replace('\n', '+', regex=True)
    datosdf = datosdf.replace('\|', '+', regex=True)
    datosdf = datosdf.replace('\r', '+', regex=True)

def selectTableToDF(tablename, esquema, engine):
    return read_sql_query('SELECT * FROM ' + esquema + '.' + tablename ,engine)

def queryTableToDF(querytable, engine):
    return read_sql_query( querytable ,engine)

def queryOracleDF(oraclesqlstatement,engine):
    try:
        connection = engine.raw_connection()
        cursor = connection.cursor()
        cursor.execute(oraclesqlstatement)
        #results = list(cursor.fetchall())
        cursor.close()
        connection.commit()
    except Exception as e:
        error = e.args
        print(error)

def runOracleProcedure(engine,ORACLE_PROCEDURE_NAME,PARAMETROS = []):
    dataresult = None
    try:
        connection = engine.raw_connection()
        cursor = connection.cursor()
        dataresult = cursor.callproc(ORACLE_PROCEDURE_NAME, PARAMETROS)
        cursor.close()
    except Exception as e:
        error = e.args
        cadena = str(error)+str(traceback.format_exc())
        print(cadena)
    return dataresult