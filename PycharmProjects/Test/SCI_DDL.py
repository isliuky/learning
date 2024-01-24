import pandas as pd
import json
from openpyxl import Workbook
import os

# 文件路径
all_file_path = r"C:\Bayer\04_SCI\new\\"
file_path = f"{all_file_path}output.xlsx"
# 检查文件是否存在
if os.path.exists(file_path):
    # 如果文件存在，删除并重建
    os.remove(file_path)
# 创建新的Excel文件
wb = Workbook()
ws = wb.active
ws.title = 'mapping'
wb.save(file_path)

excel_path = f"{all_file_path}config_read.xlsx"
excel_data = pd.read_excel(excel_path, sheet_name=None)
json_list = []
tm = "\r\n"

for sheet_name, sheet_data in excel_data.items():
    sheet_data['merge'] = sheet_data['字段名称'] + ' ' + sheet_data['字段类型']
    sheet_data['schema_table'] = sheet_data['schema名字'] + '.' + sheet_data['table名字']
    schema_table = set(sheet_data['schema_table'])
    result = ', '.join(schema_table)
    print(result)
    print(f"""
drop table if exists {result};
create table if not exists {result}(
    {f",{tm}    ".join(sheet_data['merge'].str.lower())}
    );
    """)
with open(f'{all_file_path}output.sql', 'w') as file:
    # 清除文件内容
    file.write('')
for json_data in json_list:
    print(f'成功写入entity: {json_data.get("entity")}')
    with open(f'{all_file_path}output.sql', 'a') as f:
        json.dump(json_data, f, indent=4)
        f.write(',\n')
