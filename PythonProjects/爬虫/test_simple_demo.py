import re
import pandas as pd

df_data = pd.read_excel("datas.xlsx")
room_type = [ '1房', '一室']
room_type_pattern = '|'.join(room_type)
print(df_data["title"].str.contains(room_type_pattern))