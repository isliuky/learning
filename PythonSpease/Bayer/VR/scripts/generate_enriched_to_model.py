import streamlit as st
import re

def parse_ddl(ddl):
    # Extract table name and columns from DDL.
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

def generate_sql_statements(ddl_list):
    sql_statements = []
    for ddl in ddl_list:
        target_table, columns = parse_ddl(ddl)
        if target_table and columns:
            source_table = 'enriched_stage_airepbi.userinfo'  # Replace with actual source table logic
            comment = f"-- {target_table}"
            truncate_stmt = f"truncate table {target_table};"
            insert_columns = ",\n\t".join(columns)
            select_columns = ",\n\t".join(columns)  # Assuming same columns in source and target
            insert_stmt = (
                f"insert into {target_table}(\n\t{insert_columns}\n)\n"
                f"select\n\t{select_columns}\nfrom {source_table};"
            )
            sql_statements.append(f"{comment}\n{truncate_stmt}\n{insert_stmt}")
        else:
            sql_statements.append("-- Invalid DDL statement")
    return "\n\n".join(sql_statements)

st.title("DDL to TRUNCATE and INSERT SQL Generator")

ddl_input = st.text_area("Paste your DDL statements here (one per table):", height=300)
if st.button('Generate SQL Statements'):
    ddl_list = [ddl.strip() for ddl in ddl_input.split(';') if ddl.strip()]
    sql_output = generate_sql_statements(ddl_list)
    st.code(sql_output, language='sql')