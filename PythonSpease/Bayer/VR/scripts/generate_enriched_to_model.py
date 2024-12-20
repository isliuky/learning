import streamlit as st
import re


# 解析DDL函数保持不变
def parse_ddl(ddl):
    match = re.search(r'CREATE TABLE (\w+\.\w+)', ddl)
    if not match:
        raise ValueError("Invalid DDL: Could not find table name.")

    full_table_name = match.group(1)
    schema_name, table_name = full_table_name.split('.')

    lines = ddl.strip().split('\n')[1:-1]
    lines = [line.strip() for line in lines]

    columns = []
    for line in lines:
        if not line or 'CREATE TABLE' in line or line.endswith(')'):
            continue

        parts = line.rstrip(',').split(None, 1)
        if len(parts) < 2:
            continue

        column_name = parts[0]
        rest = parts[1]

        column_type_match = re.match(r'(\w+[\s$$\d]*)', rest)
        if column_type_match:
            column_type = column_type_match.group(1).strip()
        else:
            continue

        columns.append({
            'name': column_name,
            'type': column_type
        })

    return {
        'schema': schema_name,
        'table': table_name,
        'columns': columns
    }


def generate_truncate_and_insert_statements(ddl, source_schema):
    parsed_ddl = parse_ddl(ddl)

    target_schema = parsed_ddl['schema']
    target_table = parsed_ddl['table']
    columns = parsed_ddl['columns']

    truncate_statement = f"TRUNCATE TABLE {target_schema}.{target_table};"

    column_names = ',\n    '.join([col['name'] for col in columns])
    insert_statement = (
        f"INSERT INTO {target_schema}.{target_table} (\n"
        f"    {column_names}\n"
        f")\n"
        f"SELECT\n"
        f"    {column_names}\n"
        f"FROM {source_schema}.{target_table};"
    )

    return truncate_statement, insert_statement


# Streamlit 应用程序
st.title("DDL to SQL Generator")

# 用户输入DDL
ddl_input = st.text_area("请输入DDL语句:", height=300)

# 用户输入源schema名称
source_schema = st.text_input("请输入源schema名称:", value="enriched_prestage_radacademy")

# 提交按钮
if st.button("生成SQL"):
    try:
        truncate_stmt, insert_stmt = generate_truncate_and_insert_statements(ddl_input, source_schema)

        # 显示生成的SQL语句
        st.subheader("生成的SQL语句:")
        sql_output = f"{truncate_stmt}\n\n{insert_stmt}"
        st.code(sql_output, language='sql')
    except Exception as e:
        st.error(f"发生错误: {e}")