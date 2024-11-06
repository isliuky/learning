import pandas as pd

# 读取Excel文件
file_path = r'C:\Users\EJOPV\OneDrive - Bayer\Desktop\path_to_your_excel_file.xlsx'  # 替换为您的Excel文件路径
df = pd.read_excel(file_path)

# 生成SQL语句
sql_statements = []
for index, row in df.iterrows():
    schema_name = row['schema']
    table_name = row['table']
    new_table_name = f"{table_name}_1104"

    sql_statement = f"""
    create table {schema_name}.{new_table_name}
    as
    select * from {schema_name}.{table_name};
    """
    sql_statements.append(sql_statement)

# 将生成的SQL语句保存到文件
output_file = r'C:\Users\EJOPV\OneDrive - Bayer\Desktop\generated_sql_statements.sql'
with open(output_file, 'w') as file:
    for statement in sql_statements:
        file.write(statement + '\n')

print(f"SQL statements have been written to {output_file}")