import pandas as pd
from datetime import datetime
import re
# 读取Excel文件
file_path = r'C:\Users\EJOPV\OneDrive - Bayer\Downloads\ddl 1.xlsx'
df = pd.read_excel(file_path, header=None)

# 初始化变量
tables = []
current_table = None
current_columns = []

# 遍历每一行，解析表名和列信息
for index, row in df.iterrows():
    # 检查当前行是否为空
    if row.isna().all():  # 如果所有元素都是NaN，则跳过此行
        continue

    # 确保row[0]和row[2]是字符串
    row_0 = str(row[0])
    row_2 = str(row[2])

    if row_0.startswith('目标表:'):
        if current_table:
            tables.append((current_table, current_columns))
            current_columns = []
        current_table = row_0.split(':')[1].strip()
    elif current_table:
        column_name = row_0
        column_type = str(row[1])
        from_column = row_2.lower()  # 从C列提取from字段并转换为小写
        if column_type == 'varchar(max)':
            column_type = 'varchar(65535)'  # Redshift最大varchar长度
        current_columns.append((column_name, column_type, from_column))

# 添加最后一张表
if current_table and current_columns:
    tables.append((current_table, current_columns))

# 生成DDL和INSERT语句
for table_name, columns in tables:
    staging_table_name = table_name.split(".")[1]
    print(staging_table_name)
    if re.search("cxg", table_name):
        print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        staging_table_name = table_name.split(".")[1]
        str_to_remove = 'vr_'
        staging_table_name_result = re.sub(str_to_remove, '', staging_table_name)
        print(staging_table_name_result)
    else:
        print(staging_table_name)