import pandas as pd
from datetime import datetime
import re

# Product_路径
product_file_path = r'C:\Bayer\09_VR'
# 读取Excel文件
file_path = f'{product_file_path}\ddl 1.xlsx'
df = pd.read_excel(file_path, header=None, sheet_name='Sheet1')

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

# 文件路径
ddl_file_path = f'{product_file_path}\ddl_statements.sql'
insert_file_path = f'{product_file_path}\insert_statements.sql'

# 打开文件以写入，指定编码为UTF-8
with open(ddl_file_path, 'w', encoding='utf-8') as ddl_file, open(insert_file_path, 'w', encoding='utf-8') as insert_file:
    # 生成DDL和INSERT语句
    for table_name, columns in tables:
        # 如果有表就删除
        drop = f'DROP TABLE IF EXISTS {table_name};\n'
        ddl_file.write(drop)

        # 生成DDL
        ddl = f'CREATE TABLE {table_name} (\n'
        for column_name, column_type, _ in columns:
            ddl += f'    {column_name} {column_type},\n'
        ddl = ddl.rstrip(',\n') + '\n);\n'
        ddl_file.write(ddl)
        ddl_file.write('-' * 80 + '\n')

        # 生成INSERT INTO语句
        insert_into_sql = f'INSERT INTO {table_name} (\n'
        select_sql = 'SELECT\n'
        fixed_columns = {
            'insert_dt': 'getdate()',  # 使用数据库的getdate()函数
            'insert_user': "'bayer_cn_cdp'",
            'update_dt': 'getdate()',  # 使用数据库的getdate()函数
            'update_user': "'bayer_cn_cdp'"
        }

        for column_name, column_type, from_column in columns:
            insert_into_sql += f'    {column_name},\n'
            if column_name in fixed_columns:
                select_sql += f'    {fixed_columns[column_name]},\n'
            elif column_type.lower() in ['datetime', 'timestamp']:
                select_sql += f'    TO_TIMESTAMP({from_column}, \'YYYY-MM-DD HH24:MI:SS\'),\n'
            elif column_type.lower() == 'boolean':
                select_sql += f'    CASE WHEN {from_column} = \'true\' THEN true ELSE false END,\n'
            elif column_type.lower() == 'int':
                select_sql += f'    CAST(CAST({from_column} AS FLOAT) AS INT),\n'
            else:
                select_sql += f'    CAST({from_column} AS {column_type}),\n'

        # 添加固定列
        for column_name, value in fixed_columns.items():
            if column_name not in [col[0] for col in columns]:
                insert_into_sql += f'    {column_name},\n'
                select_sql += f'    {value},\n'

        insert_into_sql = insert_into_sql.rstrip(',\n') + '\n)'
        select_sql = select_sql.rstrip(',\n')
        if re.search("cxg", table_name) or re.search("vr_account", table_name):
            staging_table_name = table_name.split(".")[1]
            str_to_remove = 'vr'
            staging_table_name_result = re.sub(str_to_remove, '', staging_table_name)
            # 生成DELETE语句
            delete_sql = f'DELETE FROM {table_name} WHERE id IN (SELECT id FROM enriched_vr.staging{staging_table_name_result});'
            insert_file.write(delete_sql)
            insert_file.write('-' * 80 + '\n')

            insert_into_sql += f'\n{select_sql}\nFROM enriched_vr.staging{staging_table_name_result};\n'
        else:
            # 生成DELETE语句
            delete_sql = f'DELETE FROM {table_name} WHERE id IN (SELECT id FROM enriched_vr.staging_{table_name.split(".")[1]});'
            insert_file.write(delete_sql)
            insert_file.write('-' * 80 + '\n')
            insert_into_sql += f'\n{select_sql}\nFROM enriched_vr.staging_{table_name.split(".")[1]};\n'
        insert_file.write(insert_into_sql)
        insert_file.write('-' * 80 + '\n')

print(f"DDL statements have been written to {ddl_file_path}")
print(f"Insert statements have been written to {insert_file_path}")