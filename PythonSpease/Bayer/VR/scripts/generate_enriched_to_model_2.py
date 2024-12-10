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
        f"FROM {source_schema};"
    )

    return truncate_statement, insert_statement


# Streamlit 应用程序
st.title("Batch DDL to SQL Generator")

# 用户输入DDL
ddl_input = st.text_area("请输入多个DDL语句 (每个DDL前一行以 '-- 源schema名称' 开头):", height=400)

# 提交按钮
if st.button("批量生成SQL"):
    try:
        # 分割DDL语句
        ddl_statements = ddl_input.split('-- ')[1:]  # 忽略第一个空元素

        sql_outputs = []
        for statement in ddl_statements:
            lines = statement.strip().split('\n')
            source_schema_line = lines[0].strip()
            ddl = '\n'.join(lines[1:]).strip()

            # if not source_schema_line.startswith('enriched_prestage_radacademy'):
            #     raise ValueError(f"无效的源schema行: {source_schema_line}")

            source_schema = source_schema_line
            truncate_stmt, insert_stmt = generate_truncate_and_insert_statements(ddl, source_schema)
            sql_outputs.append(
                f"-- Source Schema: {source_schema}\n{truncate_stmt}\n\n{insert_stmt}\n\n-- End of statements for this DDL --\n")

        # 合并所有生成的SQL语句
        all_sql_output = '\n'.join(sql_outputs)

        # 显示并提供下载链接
        st.subheader("所有生成的SQL语句:")
        st.code(all_sql_output, language='sql')

        # 提供下载选项
        st.download_button(
            label="下载SQL文件",
            data=all_sql_output,
            file_name="generated_sql.sql",
            mime="text/sql"
        )
    except Exception as e:
        st.error(f"发生错误: {e}")