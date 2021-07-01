import cx_Oracle
from sqlalchemy import create_engine
from pyspark.sql import SparkSession


def iniciaClienteOracle(rutaOracleCliente):
    cx_Oracle.init_oracle_client(lib_dir = rutaOracleCliente)

def engine_general(USUARIO, PASSWORD, IP, PUERTO, DDL):
    return  create_engine('oracle://' + USUARIO + ':' + PASSWORD + '@' + IP + ':' + PUERTO + '/' + DDL, echo=False)

def engine_spark(USUARIO, PASSWORD, IP, PUERTO, DDL):
    engine = {
        "user": USUARIO, 
        "password": PASSWORD, 
        "driver": "oracle.jdbc.driver.OracleDriver"
    }
    url = "jdbc:oracle:thin:@//"+IP+":"+PUERTO+"/"+"DDL"
    return engine,url
    #AFTER this, call like this df_csv.write.jdbc(url=url, table=table, mode=mode, properties=engine)
