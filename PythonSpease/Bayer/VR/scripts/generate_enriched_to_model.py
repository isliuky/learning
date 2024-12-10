import streamlit as st
import re

def parse_ddl(ddl):
    # 提取表名和列名。
    match = re.search(r'CREATE TABLE (\w+\.?\w*) $', ddl, re.IGNORECASE)
    if not match:
        return None, None
    table_name = match.group(1)

    columns_info = {}
    column_pattern = r'\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+([a-zA-Z]+(?:\([0-9]+$)?)(?:\s+ENCODE\s+\w+)?'
    for line in ddl.splitlines():
        match = re.match(column_pattern, line.strip())
        if match:
            column_name, _ = match.groups()
            columns_info[column_name] = True

    return table_name, list(columns_info.keys())

def generate_sql_statements(ddl_list, source_table_map):
    sql_statements = []
    for ddl in ddl_list:
        target_table, columns = parse_ddl(ddl)
        if target_table and columns:
            source_table = source_table_map.get(target_table, 'enriched_stage_airepbi.userinfo')  # 默认源表
            comment = f"-- {target_table}"
            truncate_stmt = f"truncate table {target_table};"
            insert_columns = ",\n\t".join(columns)
            select_columns = ",\n\t".join(columns)  # 假设源表和目标表具有相同的列结构
            insert_stmt = (
                f"insert into {target_table}(\n\t{insert_columns}\n)\n"
                f"select\n\t{select_columns}\nfrom {source_table};"
            )
            sql_statements.append(f"{comment}\n{truncate_stmt}\n{insert_stmt}")
        else:
            sql_statements.append("-- 无效的 DDL 语句")
    return "\n\n".join(sql_statements)

st.title("批量生成 TRUNCATE 和 INSERT SQL 语句")

# 文件上传或文本区域用于粘贴多个 DDL 语句
uploaded_file = st.file_uploader("上传包含多个 DDL 语句的文件", type=['sql', 'txt'])
if uploaded_file is not None:
    ddl_input = uploaded_file.read().decode('utf-8')
else:
    ddl_input = st.text_area("或在此处粘贴多个 DDL 语句（每个表一个）:", height=300)

source_table_map_input = st.text_area("指定源表 (格式: 目标表=源表, 每行一个):", height=100)

if st.button('生成 SQL 语句'):
    # 解析源表映射
    source_table_map = dict(line.strip().split('=') for line in source_table_map_input.splitlines() if '=' in line)

    # 将输入分割成单独的 DDL 语句
    ddl_list = [ddl.strip() for ddl in re.split(r';\s*(--.*)?', ddl_input) if ddl.strip()]

    # 生成 SQL 输出
    sql_output = generate_sql_statements(ddl_list, source_table_map)
    st.code(sql_output, language='sql')