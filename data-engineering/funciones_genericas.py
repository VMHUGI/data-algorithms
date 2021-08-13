# ----------------------------------------------------------------------------
# PR64
# Carga de Proyectos históricos ODI al Esquema Universal del Datalake
# ----------------------------------------------------------------------------
from pyspark.sql import SparkSession
from datetime import datetime
from sqlalchemy import create_engine
# ----------------------------------------------------------------------------
bd2_url = "jdbc:oracle:thin:{}/{}@{}:{}:{}".format(bd2_user,bd2_pwd,bd2_host,
                                                   bd2_port,bd2_sid)

sql_t = """SELECT
        DISTINCT 
            ieh.COD_UNICO,
            ieh.SEC_EJEC,
            CASE
                WHEN ieh.ANO_EJEC IS NOT NULL THEN ieh.ANO_EJEC
                WHEN ieh.FEC_VERSION IS NULL THEN EXTRACT(YEAR FROM ieh.FEC_MODI)
                ELSE EXTRACT(YEAR FROM ieh.FEC_VERSION)
            END ANO_EJEC,
            -- ieh.ANO_EJEC,
            ieh.UNIDAD_OPMI AS UNIDAD_OPMI_EJE,
            ieh.UNIDAD_UEI AS UNIDAD_UEI_EJE, 
            ieh.UNIDAD_UF AS UNIDAD_UF,
            ip.UNIDAD_OPMI AS UNIDAD_OPMI_FYE,
            ip.UNIDAD_UEI AS UNIDAD_UEI_FYE,
            ip.UNIDAD_UF AS UNIDAD_UF_FYE,
            CASE
                WHEN ieh.FEC_MODI IS NULL THEN  ieh.FEC_VERSION
                ELSE ieh.FEC_MODI
            END AS FEC_TRANSACCION
    FROM ODI.INV_EJECUCION_H ieh
    LEFT JOIN ODI.INV_PROYECTO ip ON ip.ID_PROYECTO = ieh.ID_INVERSION
"""

spark = SparkSession.builder.master("local[*]").enableHiveSupport()\
    .appName('SparkApp').getOrCreate()

def load_oracle_to_spark_df(bd2_url,sql_t):
    '''Hacemos la conexión de la data a importar en un spark df
    mediante el url de conexión y el query a consultar'''
    
    print('\nSTART: ' + datetime.now().strftime("%d/%m/%Y-%H:%M:%S"))
    df_aggr = spark.read.format("jdbc").option("url", bd2_url)\
        .option("query", sql_t).load()    
    print('END: ' + datetime.now().strftime("%d/%m/%Y-%H:%M:%S"))

    return df_aggr

# ----------------------------------------------------------------------------

bd1_url_spk = "jdbc:oracle:thin:@//{}:{}/{}".format(bd1_host, bd1_port,bd1_sid)
bd1_table = 'DM_PROYECTO_HIST'
mode = 'append'
props = {"user": bd1_user,
        "password": bd1_pwd,
        "driver": "oracle.jdbc.driver.OracleDriver",
        "chunksize":str(chunksize)}

def load_spark_df_to_oracle(bd1_url_spk,bd1_table,mode,props,df_aggr):
    '''Hacemos la conexión con spark para exportar la data
    a la base de datos destino.'''
    
    print('\nSTART: ' + datetime.now().strftime("%d/%m/%Y-%H:%M:%S"))
    df_aggr.write.jdbc(url = bd1_url_spk, table=bd1_table,
                        mode=mode, properties=props)
    print('\nEND: ' + datetime.now().strftime("%d/%m/%Y-%H:%M:%S"))

# ----------------------------------------------------------------------------
bd1_sch = 'SCH_UNIVERSAL'
bd1_table = 'DM_PROYECTO_HIST'
bd1_url = 'oracle://{}:{}@{}:{}/{}'.format(bd1_user,bd1_pwd,bd1_host,
                                           bd1_port,bd1_sid)

def truncate_oracle_table(bd1_url,bd1_sch,bd1_table):
    '''Función que trunca la tabla ubicada en el esquema definido'''
    
    #Conexion
    db = create_engine(bd1_url, echo=False)
    conn = db.connect()
        
    #Limpiamos la tabla
    startTime = datetime.now()
    conn.execute("TRUNCATE TABLE {}.{}".format(bd1_sch,bd1_table))
    conn.close()
    db.dispose()
    print('Tiempo de limpieza de tabla: ',datetime.now() - startTime)
# ----------------------------------------------------------------------------
# PR65
# Carga STD_ENTRADAS
# ----------------------------------------------------------------------------
from datetime import date, timedelta,datetime

def Dias():
    '''Condicional de ejecución en base al día'''
    
    #Si hoy es lunes
    if date.today().weekday() == 0:
        dia_reporte = (date.today() - timedelta(days=2))
    #Si es domingo
    elif date.today().weekday() == 6:
        dia_reporte = (date.today() - timedelta(days=1))
    #Cualquier otro dia
    else:
        dia_reporte = date.today()
        
    #Dia hábil -1
    yesterday = (dia_reporte - timedelta(days=1)).strftime('%b-%d')
    
    if date.today().weekday() == 1:
        day_before= (dia_reporte - timedelta(days=4)).strftime('%b-%d')
    else:
        day_before= (dia_reporte - timedelta(days=2)).strftime('%b-%d')
    
    # Para recuperar la información que el usuario llenó el día anterior
    if date.today().weekday() == 2:
        two_ago= (dia_reporte - timedelta(days=5)).strftime('%b-%d')
    elif date.today().weekday() == 1:
        two_ago= (dia_reporte - timedelta(days=5)).strftime('%b-%d')
    else:
        two_ago= (dia_reporte - timedelta(days=3)).strftime('%b-%d')
        
    return dia_reporte, yesterday, day_before, two_ago

# ----------------------------------------------------------------------------
# PR72
# Carga de datos de operadores móviles - MTC
# ----------------------------------------------------------------------------
import os 

output = r'E:\PORTAFOLIO_EXTERNO\MTC\NEW_PARSED'
new_err = r'E:\PORTAFOLIO_EXTERNO\MTC\NEW_ERROR'
file_out = r'E:\PORTAFOLIO_EXTERNO\MTC\NEW_PARQUET'

def delete_local_files():
        os.chdir(output)
        for file in os.listdir(output):
            os.remove(file)

        os.chdir(file_out)
        for file in os.listdir(file_out):
            os.remove(file)
        
        os.chdir(new_err)
        for file in os.listdir(new_err):
            os.remove(file)

# ----------------------------------------------------------------------------
# PR77
# Transformación de SIGA DONACIONES
# ----------------------------------------------------------------------------
import pandas as pd 

filename1 = r'E:\MEF\DATA\DATA_SIGA_DONACIONES.xlsx'

def load_excel_to_pandas_df(filename1):
    
    data = pd.read_excel(filename1,encoding='latin10')
    return data

def save_pandas_df_to_excel(data):

    data.to_excel("DATA_ROW_FLAG.xlsx",encoding="latin10", index = False)

# ----------------------------------------------------------------------------
# PR78
# Carga de DONACION SIGA
# ----------------------------------------------------------------------------
import pygsheets

path_credentials = r'E:\KEY_DRIVE'
credentials_file =  'client.json'

def connect_pygsheets(path_credentials,credentials_file):

    gc = pygsheets.authorize(service_file= path_credentials + '\\' + credentials_file)

    return gc

# ----------------------------------------------------------------------------
# PR79
# ETL 2020 Gasto - Activación
# ----------------------------------------------------------------------------
import ftplib, shutil

state_final = 0
state_file = 0
year = '2020'

def validate(state_file, state_final):
    counter_success = 0
    counter_fail = 0
    print(threading.TIMEOUT_MAX)
    while (threading.TIMEOUT_MAX < 3 and state_final == 0):
        chosen_file = names[14]
        date_file = ftp.voidcmd("MDTM " + chosen_file)
        if (chosen_file == year + '.zip' and date_file[4:12] == date_now):
            counter_success = counter_success + 1
            state_file = 1
        else:
            state_file = 0
            counter_fail = counter_fail + 1
            time.sleep(20.0)
        state_final = state_file
    print(str(counter_fail)+threading.current_thread().getName())
    return state_final

# ----------------------------------------------------------------------------
# PR99
# Generación de variables - BBSS
# ----------------------------------------------------------------------------
import datetime as dt

def execute_sql_file(sql):
        try:
            with engine.connect().execution_options(autocommit=True) as conn:
                conn.execute(text(sql))
        except Exception as e:
            with open(file_reportg, 'a') as f:
                    error = e.args
                    f.write('ERROR  :' + dt.datetime.now().strftime("%d/%m/%Y-%H:%M:%S") + '\n')
                    f.write(str(error) + '\n')

def read_sql_file(sql_file):
    with open(sql_file, 'r') as f:
        output = f.read()
    return output

# ----------------------------------------------------------------------------
# PR100
# Carga SIGA TRANSMISION
# ----------------------------------------------------------------------------
import pandas as pd
from sqlalchemy import types
from datetime import datetime, create_engine

def definedatastructure_pandas_df_to_oracle(dfparam):
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "int" in str([type(v) for v in dfparam[i].values]):
            dtypedict.update({i: types.INT()})
        if "float" in str([type(v) for v in dfparam[i].values]):
            dtypedict.update({i: types.Float(precision=9, asdecimal=True)})
        if "datetime" in str([type(v) for v in dfparam[i].values]):
            dtypedict.update({i: types.DateTime()})
        if "NoneType" in str([type(v) for v in dfparam[i].values]):
            dtypedict.update({i: types.VARCHAR(1)})##columna vacía
        if "object" in str([type(v) for v in dfparam[i].values]):
            dtypedict.update({i: types.NVARCHAR(max([len(str(n)) for n in dfparam[i].values]))})
        if "str" in str([type(v) for v in dfparam[i].values]):
            dtypedict.update({i: types.VARCHAR(max([len(str(n)) for n in dfparam[i].values]))})
    return dtypedict


bd2_url = 'oracle://{}:{}@{}:{}/{}'.format(bd2_user,bd2_pwd,bd2_host,
                                                   bd2_port,bd2_sid)

sql_t = """select lx.*, case when lx.fecha=to_char(sysdate,'dd/mm/yyyy') then 'SI' else 'NO' end ENVIO_HOY, '' cantidad
from ( select l.i_trans_anno trans_anno,l.i_sec_ejec sec_ejec, --l.s_version, -- l.s_archivo_original,
       to_char(max(l.dt_generacion_fecha),'dd/mm/yyyy') fecha,
       --to_char(l.dt_proceso_inicio,'dd/mm/yyyy') fecha,
       to_char(max(l.dt_proceso_inicio),'hh24:mi:ss') hora --, count(*)
       from siga_transmision_DU.VW_TRANSMISION_LOG l where l.i_trans_anno>=2020 --and i_sec_ejec=1345
       group by l.i_trans_anno,l.i_sec_ejec --,l.s_version
       -- order by 3 desc 
    ) lx"""

def load_oracle_to_pandas_df(sql_t,bd2_url):
    
    startTime = datetime.now()
    db = create_engine(bd2_url, echo=False)
    conn = db.connect()
    
    df_from_oracle = pd.read_sql_query(sql_t, conn)
    df_from_oracle.columns = df_from_oracle.columns.str.upper()
    
    conn.close()
    db.dispose()
    print('Tiempo de importación: ',datetime.now() - startTime)
    
    return df_from_oracle

# ----------------------------------------------------------------------------
# PR111
# Carga SIGA TRANSMISION
# ----------------------------------------------------------------------------
import pysftp
import stat

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

def list_ftp_recursively(sftp,attrInicial,remoteFilePath):
    if(stat.S_ISDIR(attrInicial.st_mode)):
        sftp.cwd(remoteFilePath)
        directory_structure = sftp.listdir_attr()
        for attr in directory_structure:
            if(stat.S_ISDIR(attrInicial.st_mode)):
                #print(remoteFilePath+attr.filename + ' --> ' + localFilePath+attr.filename)
                list_ftp_recursively(sftp,attr,remoteFilePath + '/'+ attr.filename )
    elif(stat.S_ISREG(attrInicial.st_mode)):
        print(remoteFilePath+' ... Dowloading...',end='.')
        sftp.get(remoteFilePath, localFilePath+'\\'+attrInicial.filename)
        print('Done')

def connect_to_sftp():
    with pysftp.Connection(host=mysite, username=username, password=password, private_key=".ppk", cnopts=cnopts) as sftp:
        print ("Connection succesfully stablished ... ")
        remoteFilePath = '/MEF/'
        directory_structure = sftp.listdir_attr('/MEF/')
        for attr in directory_structure:
            if(attr.filename == "Diaria"):
                list_ftp_recursively(sftp,attr,'/MEF/Diaria')
                #print(remoteFilePath+attr.filename + ' --> ' + localFilePath+attr.filename)
                #sftp.get(remoteFilePath+attr.filename, localFilePath+'\\'+attr.filename)
        sftp.close()

# ----------------------------------------------------------------------------
# PR120
# ETL 2021 Gasto - Carga
# ----------------------------------------------------------------------------
import os
from os import listdir
from os.path import isfile, join

def remove_files():
        source_path = r'E:\CONSULTA_AMIGABLE_GASTO_DIARIO'
        contenido_csv = os.listdir(source_path)
        year = '2021'
        files_ruta = [(source_path + '\\' + year + '.ZIP')]

        for csv_files in contenido_csv:
            try:
                if os.path.isfile(os.path.join(source_path, csv_files)) and csv_files.endswith(
                        '.ZIP') and csv_files == year + '.ZIP':
                    os.remove(os.path.join(source_path, csv_files))
                if os.path.isfile(os.path.join(source_path, csv_files)) and csv_files.endswith(
                        '.csv') and csv_files == year + '.csv':
                    os.remove(os.path.join(source_path, csv_files))
            except FileExistsError:
                print("Error en la existencia del Archivo")

import time

def update_stats():
    script = "BEGIN DBMS_STATS.GATHER_TABLE_STATS(\'SCH_UNIVERSAL\',\'CONS_AMIG_GASTO_DIARIO_2021\'); END;"
    engine.execute(script)
    print('finish_stats:' + time.strftime("%d/%m/%Y-%H:%M:%S", time.localtime()))

# ----------------------------------------------------------------------------
# PR124
# LECTURA DE RECURSOS SERVER & BD
# ----------------------------------------------------------------------------
from sqlalchemy import types, create_engine
from sqlalchemy import text
import shutil
import psutil
import datetime
import pandas as pd
import socket
import datetime as dt
from os.path import isfile, join
import os

def read_stats_server():
    table_name = "ugi_td_servidor_recursos"

    engine = Conn().conn() #

    dtype_df = {'HOST': types.VARCHAR(length=50), 'IP': types.VARCHAR(length=50),
                'CPU': types.Float(precision=2, asdecimal=True), 'RAM': types.Float(precision=2, asdecimal=True),
                'DISK_E': types.INTEGER, 'DISK_C': types.INTEGER, 'FREE_E': types.INTEGER, 'FREE_C': types.INTEGER}

    sql_orcl = 'SELECT TBS_FREE_MB FROM SCH_UNIVERSAL.VW_UGI_MONITOREO_BD'

    file_reportex_root = r'E:\PORTAFOLIO\CONTROL_RECURSOS\SCRIPTS\LOG_Exception'
    file_reportex_name = dt.datetime.now().strftime('%Y%m%d') + 'reporteException.txt'
    path_file_reportex = file_reportex_root + '\\' + file_reportex_name

    FECHA = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    cpu = psutil.cpu_percent()
    host_name = socket.gethostname()
    ip_addr = socket.gethostbyname(socket.gethostname())
    ram = psutil.virtual_memory().percent
    totalE, usedE, freeE = shutil.disk_usage("E:")
    totalC, usedC, freeC = shutil.disk_usage("C:")

    data = pd.DataFrame(columns=['HOST', 'IP', 'CPU', 'RAM', 'DISK_E', 'DISK_C', 'FREE_E', 'FREE_C'],
                        data=[[host_name, ip_addr, cpu, ram, totalE, totalC, freeE, freeC]])

    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql_orcl))
            for row in result:
                orcl_mb = row['tbs_free_mb']
        # print(orcl_mb)
        ind_free_e = round(freeE / 2 ** 30, 1)
        ind_free_c = round(freeC / 2 ** 30, 1)

        if ind_free_e < 20 or ind_free_c < 10 or cpu > 85 or ram > 85 or orcl_mb <= 5120:
            with open(path_file_reportex, 'a') as f:
                f.write('*****************************' + '\n')
                f.write('ESPACIOS LIBRE EN DISCO - ' + dt.datetime.now().strftime("%d/%m/%Y-%H:%M:%S") + '\n')
                f.write('*****************************' + '\n')
                f.write('UNIDAD C: ' + str(ind_free_c) + ' GB\n')
                f.write('UNIDAD E: ' + str(ind_free_e) + ' GB\n')
                f.write('*****************************' + '\n')
                f.write('CONSUMO DE RAM Y CPU - ' + dt.datetime.now().strftime("%d/%m/%Y-%H:%M:%S") + '\n')
                f.write('*****************************' + '\n')
                f.write('CPU:   ' + str(cpu) + ' %\n')
                f.write('RAM:   ' + str(ram) + ' %\n')
                f.write('*****************************' + '\n')
                f.write('ESPACIO LIBRE EN TABLESPACE - ' + dt.datetime.now().strftime("%d/%m/%Y-%H:%M:%S") + '\n')
                f.write('*****************************' + '\n')
                f.write('TABLESPACE:   ' + str(round(orcl_mb / 2 ** 10, 1)) + ' GB\n')

        data.to_sql(name=table_name,
                    con=engine,
                    schema='SCH_UNIVERSAL',
                    if_exists='append',
                    index=False,
                    dtype=dtype_df)

    except Exception as e:
        erro = e.args
        with open(path_file_reportex, 'a') as f:
            f.write('ERROR EXCEPTION  :' + dt.datetime.now().strftime("%d/%m/%Y-%H:%M:%S") + '\n')
            f.write(str(erro) + '\n')

# ----------------------------------------------------------------------------
# PR129
# Carga de data CONOSCE
# ----------------------------------------------------------------------------
import pandas as pd

def loadtable(db_table):
    db_table.to_sql(name='est_conosce_invierte', con=engine, if_exists='replace', index=False, dtype=dtyp2, chunksize = CHUNKZIZE)

# ----------------------------------------------------------------------------
# PR157
# Carga archivo ftp minam al RAW_UGI
# ----------------------------------------------------------------------------
import ftplib
from ftplib import FTP

# Credenciales conexión
site_address = '10.1.1.1'
user = 'user'
password = 'password'

def login_ftp(site_address,user, password):
    try:
        ftp = FTP(site_address)
        ftp.login(user=user, passwd = password)
        wd = ftp.pwd()
        print('\n\t\t\t\tftp - Login correct')
        print('Directorio actual: ', wd)

    except ftplib.all_errors as e:
        print('FTP error:', e)

    return ftp

# ----------------------------------------------------------------------------
# PR163
# PR_RENIEC_PN_RN_load_from_file_to_raw_table_Oracle
# ----------------------------------------------------------------------------
import patoolib
import os
import shutil

localFilePathAlm = r'E:\MEF\DATA'

def descompress_rar_to_csv(filepath):
    rarfiles_list = glob.glob(filepath + '*.rar')
    if rarfiles_list is None:
        pass
    else:
        for r in rarfiles_list:
            try:
                patoolib.extract_archive(r, outdir=filepath)
                print('Archivo rar descomprimido')
                shutil.move(r, localFilePathAlm + os.path.basename(r))
            except Exception as e:    
                print('Error al descomprimir el archivo: ', e)