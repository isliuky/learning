import pandas as pd
import json
from openpyxl import Workbook
import os

# 文件路径
all_file_path = r"C:\Bayer\03_Opera\new\\"
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
id=51826
for sheet_name, sheet_data in excel_data.items():
    print(sheet_name)
    if sheet_data['API Name'].str.contains('systemmodstamp', case=False).any():
        sql_query = f"select {','.join(list(sheet_data['API Name'].str.lower()))} from {sheet_name.lower()}"
        standard_columns = ",".join(list(sheet_data['API Name'].str.lower()))
    else:
        sql_query = f"select {','.join(list(sheet_data['API Name'].str.lower()))},systemmodstamp from {sheet_name.lower()}"
        standard_columns = ",".join(list(sheet_data['API Name'].str.lower())) + ',' + 'systemmodstamp'
    json_data = {
        "domain": "enriched_ce",
        "entity": f'{sheet_name.lower()}_tmp',
        "delimiter": ",",
        "file_inzip_pattern": "",
        "file_inzip_suffix": "",
        "is_archive": "N",
        "is_exchange_merge": "false",
        "is_header": "true",
        "is_signal_file": "false",
        "is_soft_fail": "true",
        "landing_file_format": "csv",
        "merge_order_cols": "",
        "merge_order_sc": "desc",
        "primary_keys": "",
        "redshift_enriched_post_job": f"TRUNCATE TABLE enriched_ce.{sheet_name.lower()}_tmp",
        "salesforce_identifier": "opera2",
        "salesforce_name": f"{sheet_name.lower()}",
        "skip_row": "0",
        "source_system": "salesforce",
        "sql_query": sql_query,
        "standard_columns": standard_columns,
        "state_machine_name": "ph-cdp-sm-workflow-cn-etl_ce_data_load",
        "source_sensor_poke_interval": "60",
        "source_sensor_retry_time": "1",
        "time_delta": "0*60",
        "use_cols": ""
    }
    json_list.append(json_data)
    # print(sheet_data['API Name'])
    mapping_df = pd.DataFrame(sheet_data['API Name'], index=sheet_data.index)  # 保留原来的行索引
    mapping_df = mapping_df.rename(columns={'API Name': 'source_column_name'})
    mapping_df['source_column_name'] = mapping_df['source_column_name'].str.lower()
    mapping_df['is_active'] = 'Y'
    if sheet_data['API Name'].str.contains('systemmodstamp', case=False).any():
        sys_df = pd.DataFrame([
            ['sys_record_id', 'N'],
            ['sys_primary_key', 'N'],
            ['sys_created_dt', 'Y'],
            ['sys_created_id', 'N'],
            ['sys_data_source', 'N'],
            ['sys_tenant', 'N'],
            ['sys_delete_flag', 'N'],
            ['sys_src_modified_dt', 'N']
        ], columns=mapping_df.columns)
    else:
        sys_df = pd.DataFrame([
            ['systemmodstamp', 'Y'],
            ['sys_record_id', 'N'],
            ['sys_primary_key', 'N'],
            ['sys_created_dt', 'Y'],
            ['sys_created_id', 'N'],
            ['sys_data_source', 'N'],
            ['sys_tenant', 'N'],
            ['sys_delete_flag', 'N'],
            ['sys_src_modified_dt', 'N']
        ], columns=mapping_df.columns)

    mapping_df = pd.concat([mapping_df, sys_df], ignore_index=True)
    mapping_df['source_domain'] = 'enriched_ce'
    mapping_df['source_table'] = f'{sheet_name.lower()}_tmp'
    mapping_df['source_data_type'] = 'varchar'
    mapping_df['target_database'] = 'cn_cdp_dev'
    mapping_df['target_schema'] = 'enriched_ce'
    mapping_df['target_table'] = f'{sheet_name.lower()}_tmp'
    mapping_df['target_column_name'] = mapping_df['source_column_name'].str.lower()
    mapping_df['target_data_type'] = 'nvarchar(1000)'
    mapping_df['is_null'] = 'Y'
    mapping_df['derive_desc'] = ''
    
    mapping_df = mapping_df.assign(id= id + mapping_df.index)
    mapping_df = mapping_df.assign(column_sequence=mapping_df.index + 1)
    mapping_df = mapping_df.reindex(columns=
                                    ['id', 'source_domain', 'source_table', 'source_column_name',
                                     'source_data_type', 'target_database', 'target_schema',
                                     'target_table', 'target_column_name', 'target_data_type',
                                     'column_sequence', 'is_null', 'is_active', 'derive_desc'
                                     ])
    # print(mapping_df)
    writer = pd.ExcelWriter(f'{all_file_path}output.xlsx', engine='openpyxl', mode='a')
    mapping_df.to_excel(writer, sheet_name=f'{sheet_name}', index=False)
    writer.save()

with open(f'{all_file_path}output.json', 'w') as file:
    # 清除文件内容
    file.write('')
for json_data in json_list:
    print(f'成功写入entity: {json_data.get("entity")}')
    with open(f'{all_file_path}output.json', 'a') as f:
        json.dump(json_data, f, indent=4)
        f.write(',\n')
