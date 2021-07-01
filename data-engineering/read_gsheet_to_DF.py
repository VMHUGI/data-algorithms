import gspread
import pandas as pd

KEYID = 'insertar el ID aqui'

gc = gspread.service_account()
datareference = gc.open_by_key(KEYID).get_worksheet(0)
datasheet = pd.DataFrame(datareference.get_all_records())

print(datasheet)