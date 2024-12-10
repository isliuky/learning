import re


def parse_ddl(ddl):
    # Extract schema and table name
    match = re.search(r'CREATE TABLE (\w+\.\w+)', ddl)
    if not match:
        raise ValueError("Invalid DDL: Could not find table name.")

    full_table_name = match.group(1)
    schema_name, table_name = full_table_name.split('.')

    # Split the DDL into lines and skip the first line (CREATE TABLE) and the last line (ending with ))
    lines = ddl.strip().split('\n')[1:-1]

    # Remove any leading or trailing whitespace from each line
    lines = [line.strip() for line in lines]

    # Extract columns and their types
    columns = []
    for line in lines:
        # Skip empty lines
        if not line:
            continue

        # Remove trailing commas and split by the first space to separate column name and type
        parts = line.rstrip(',').split(None, 1)
        if len(parts) < 2:
            continue  # Skip lines that do not contain a valid column definition

        column_name = parts[0]
        rest = parts[1]

        # Extract column type and encoding
        column_type_match = re.match(r'(\w+[\s$$\d]*)', rest)
        if column_type_match:
            column_type = column_type_match.group(1).strip()
        else:
            continue  # Skip if we can't extract the column type

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

    # Generate TRUNCATE statement
    truncate_statement = f"TRUNCATE TABLE {target_schema}.{target_table};"

    # Generate INSERT statement with proper formatting
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


def main():
    ddl = """
    CREATE TABLE model_dw_radacademy.bayer_comments (
        id character varying(255) ENCODE lzo,
        content_id integer ENCODE az64,
        type integer ENCODE az64,
        user_id integer ENCODE az64,
        openid character varying(765) ENCODE lzo,
        comment character varying(65535) ENCODE lzo,
        zan integer ENCODE az64,
        status integer ENCODE az64,
        approver_id integer ENCODE az64,
        approver_time timestamp without time zone ENCODE az64,
        created_at timestamp without time zone ENCODE az64,
        updated_at timestamp without time zone ENCODE az64,
        unionid character varying(300) ENCODE lzo,
        sys_created_dt character varying(255) ENCODE lzo
    ) DISTSTYLE AUTO;
    """

    source_schema = "enriched_prestage_radacademy"
    truncate_stmt, insert_stmt = generate_truncate_and_insert_statements(ddl, source_schema)

    print("Generated SQL Statements:")
    print(truncate_stmt)
    print("\n" + insert_stmt)


if __name__ == '__main__':
    main()