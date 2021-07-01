from base64 import encode
from pickle import FALSE
from pandas import ExcelWriter, read_csv, read_excel

def DFToXLSX(dfdatos,filenamepath,sheetname):
    writer = ExcelWriter(filenamepath, engine='xlsxwriter', datetime_format='dd/mm/yy',date_format='dd/mm/yy')
    dfdatos.to_excel(writer, sheet_name=sheetname, index = False)
    writer.save()

def XLSXtoDF(filenamepath,sheetname):
    return read_excel(filenamepath, sheet_name=sheetname)

def CSVtoDF(filepath, separador = ','):
    return read_csv(filepath, sep = separador)

def DFtoCSV(dfdatos,filenamepath):
    #dfdatos.to_csv(filenamepath, sep=",", mode='a', header=True, encoding = "utf-8")
    dfdatos.to_csv(filenamepath, sep=",", encoding = "utf-8", index=False)

def stringlogToText(logstring,filenamepath):
    with open(filenamepath, 'a') as filelog:
        filelog.write(logstring)