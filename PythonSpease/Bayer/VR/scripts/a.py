import re


# 解析DDL函数保持不变
def parse_ddl(ddl):
    match = re.search(r'CREATE TABLE (\w+\.\w+)', ddl)
    if not match:
        raise ValueError("Invalid DDL: Could not find table name.")
    # print(match)
    full_table_name = match.group(1)
    # print(full_table_name)
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
aaa = """
-- enriched_stage_airepbi.business_opportunity
CREATE TABLE model_dw_airepbi.business_opportunity (
    name character varying(1000) ENCODE lzo,
    businesscategoryname character varying(1000) ENCODE lzo,
    productname character varying(1000) ENCODE lzo,
    saleprocessname character varying(1000) ENCODE lzo,
    externalusername character varying(1000) ENCODE lzo,
    externaluserid character varying(1000) ENCODE lzo,
    unionid character varying(1000) ENCODE lzo,
    corpid character varying(1000) ENCODE lzo,
    corpname character varying(1000) ENCODE lzo,
    corphiscode character varying(1000) ENCODE lzo,
    createtime character varying(1000) ENCODE lzo,
    wxworkuserid character varying(1000) ENCODE lzo,
    masterusername character varying(1000) ENCODE lzo,
    flowuserid character varying(1000) ENCODE lzo,
    flowusername character varying(1000) ENCODE lzo,
    presaleamount character varying(1000) ENCODE lzo,
    presigndate character varying(1000) ENCODE lzo,
    customfield character varying(65535) ENCODE lzo,
    sys_created_dt character varying(255) ENCODE lzo,
    is_submit_application_form character varying(1000) ENCODE lzo,
    etl_create_time character varying(200) ENCODE lzo,
    etl_update_time character varying(200) ENCODE lzo,
    etl_create_by character varying(200) ENCODE lzo,
    etl_update_by character varying(200) ENCODE lzo
) DISTSTYLE AUTO;
-- enriched_prestage_airepbi.corpinfo
CREATE TABLE model_dw_airepbi.corpinfo (
    id character varying(1000) ENCODE lzo,
    corpname character varying(1000) ENCODE lzo,
    corpnickname character varying(1000) ENCODE lzo,
    corptel character varying(1000) ENCODE lzo,
    corpaddress character varying(1000) ENCODE lzo,
    corpwebsite character varying(1000) ENCODE lzo,
    industrytypeid character varying(1000) ENCODE lzo,
    industrytypestr character varying(1000) ENCODE lzo,
    corpsize character varying(1000) ENCODE lzo,
    corpsizestr character varying(1000) ENCODE lzo,
    corpcity character varying(1000) ENCODE lzo,
    corpprovince character varying(1000) ENCODE lzo,
    corpcitystr character varying(1000) ENCODE lzo,
    corplevel character varying(1000) ENCODE lzo,
    createusername character varying(1000) ENCODE lzo,
    updateusername character varying(1000) ENCODE lzo,
    feed character varying(1000) ENCODE lzo,
    last_updt_time character varying(1000) ENCODE lzo,
    ctmonthlyenhanceexaminationtotal character varying(1000) ENCODE lzo,
    ctenhancementrate character varying(1000) ENCODE lzo,
    mrmonthlyenhanceexaminationtotal character varying(1000) ENCODE lzo,
    mrienhancementrate character varying(1000) ENCODE lzo,
    hospitalrank character varying(1000) ENCODE lzo,
    hospitalrankstr character varying(1000) ENCODE lzo,
    chestpaincentercertificationtype character varying(1000) ENCODE lzo,
    chestpaincentercertificationtypestr character varying(1000) ENCODE lzo,
    checkpart character varying(1000) ENCODE lzo,
    checkpartstr character varying(1000) ENCODE lzo,
    affiliatedmedicalassociation character varying(1000) ENCODE lzo,
    departmentdevelopmentdirection character varying(1000) ENCODE lzo,
    departmentdevelopmentdirectionstr character varying(1000) ENCODE lzo,
    businesscode character varying(1000) ENCODE lzo,
    decil character varying(1000) ENCODE lzo,
    iscpa character varying(1000) ENCODE lzo,
    businessregion character varying(1000) ENCODE lzo,
    businessmanager character varying(1000) ENCODE lzo,
    represent character varying(1000) ENCODE lzo,
    uvsales character varying(1000) ENCODE lzo,
    mvsales character varying(1000) ENCODE lzo,
    gvsales character varying(1000) ENCODE lzo,
    pvsales character varying(1000) ENCODE lzo,
    potentialhospital character varying(1000) ENCODE lzo,
    potential_hospital character varying(1000) ENCODE lzo,
    hospitaltype character varying(65535) ENCODE lzo,
	etl_create_time character varying(200) ENCODE lzo,
    etl_update_time character varying(200) ENCODE lzo,
    etl_create_by character varying(200) ENCODE lzo,
    etl_update_by character varying(200) ENCODE lzo
) DISTSTYLE AUTO;
-- enriched_stage_airepbi.airep_province_mapping
CREATE TABLE model_dw_airepbi.airep_province_mapping (
		province character varying(600) ENCODE lzo,
		region character varying(600) ENCODE lzo,
        ai_rep character varying(600) ENCODE lzo,
        start_date character varying(600) ENCODE lzo,
        end_date character varying(600) ENCODE lzo,
        updatetime timestamp without time zone ENCODE az64,
        cwid character varying(600) ENCODE lzo,
        email character varying(600) ENCODE lzo,
		etl_create_time character varying(200) ENCODE lzo,
		etl_update_time character varying(200) ENCODE lzo,
		etl_create_by character varying(200) ENCODE lzo,
		etl_update_by character varying(200) ENCODE lzo
) DISTSTYLE AUTO;
-- enriched_stage_airepbi.hospital_equipment
CREATE TABLE model_dw_airepbi.hospital_equipment (
    id character varying(65535) ENCODE lzo,
    createtime character varying(1000) ENCODE lzo,
    updatetime character varying(200) ENCODE lzo,
    wxworkuserid character varying(1000) ENCODE lzo,
    wxworkusername character varying(1000) ENCODE lzo,
    biztype character varying(1000) ENCODE lzo,
    externaluserid character varying(1000) ENCODE lzo,
    externalusername character varying(1000) ENCODE lzo,
    externalrealname character varying(1000) ENCODE lzo,
    externaluserremark character varying(1000) ENCODE lzo,
    chatid character varying(1000) ENCODE lzo,
    chatname character varying(1000) ENCODE lzo,
    corpid character varying(1000) ENCODE lzo,
    corpname character varying(1000) ENCODE lzo,
    corphiscode character varying(1000) ENCODE lzo,
    bizrecordtempid character varying(1000) ENCODE lzo,
    bizrecordtempname character varying(1000) ENCODE lzo,
    relationtype character varying(1000) ENCODE lzo,
    wecomsessionid character varying(1000) ENCODE lzo,
    devices_num character varying(200) ENCODE lzo,
    host_brand_1 character varying(200) ENCODE lzo,
    high_injection_brand character varying(200) ENCODE lzo,
    host_brand_2 character varying(200) ENCODE lzo,
    high_injection_model character varying(200) ENCODE lzo,
    host_model character varying(200) ENCODE lzo,
    ccpsrecord character varying(65535) ENCODE lzo,
    lastupdttime character varying(1000) ENCODE lzo,
    etl_create_time character varying(200) ENCODE lzo,
    etl_update_time character varying(200) ENCODE lzo,
    etl_create_by character varying(200) ENCODE lzo,
    etl_update_by character varying(200) ENCODE lzo
) DISTSTYLE AUTO;
-- enriched_stage_airepbi.internal_ai_rep_hosp_list
CREATE TABLE model_dw_airepbi.internal_ai_rep_hosp_list (
    hospital character varying(150) ENCODE lzo,
    hospital_code character varying(150) ENCODE lzo,
    hospital_label character varying(150) ENCODE lzo,
    updatetime timestamp without time zone ENCODE az64,
	etl_create_time character varying(200) ENCODE lzo,
    etl_update_time character varying(200) ENCODE lzo,
    etl_create_by character varying(200) ENCODE lzo,
    etl_update_by character varying(200) ENCODE lzo
) DISTSTYLE AUTO;
-- enriched_stage_airepbi.userinfo
CREATE TABLE model_dw_airepbi.userinfo (
    userid character varying(150) ENCODE lzo,
    name character varying(150) ENCODE lzo,
    sex character varying(150) ENCODE lzo,
    mobile character varying(150) ENCODE lzo,
    phone character varying(150) ENCODE lzo,
    created character varying(150) ENCODE lzo,
    email character varying(150) ENCODE lzo,
    responsible character varying(150) ENCODE lzo,
    communicationstage character varying(65535) ENCODE lzo,
    weworkname character varying(65535) ENCODE lzo,
    is_recruit character varying(150) ENCODE lzo,
    businessname character varying(600) ENCODE lzo,
    businessprovince character varying(600) ENCODE lzo,
    businessregion character varying(600) ENCODE lzo,
    businesscode character varying(600) ENCODE lzo,
    businessmanager character varying(600) ENCODE lzo,
    followupcount integer ENCODE az64,
    topicofinterest character varying(600) ENCODE lzo,
    sign character varying(600) ENCODE lzo,
    subscribe character varying(600) ENCODE lzo,
    iswechatfriend character varying(150) ENCODE lzo,
    executive character varying(150) ENCODE lzo,
    medicalcaretype character varying(150) ENCODE lzo,
    departments character varying(150) ENCODE lzo,
    doctorcode character varying(150) ENCODE lzo,
    businesscity character varying(150) ENCODE lzo,
    feed character varying(65535) ENCODE lzo,
    last_updt_time character varying(150) ENCODE lzo,
    cwid character varying(60) ENCODE lzo,
    feedtype character varying(60) ENCODE lzo,
    isemployee character varying(60) ENCODE lzo,
    isimportant character varying(60) ENCODE lzo,
    ispotential character varying(60) ENCODE lzo,
    leadstatus character varying(10) ENCODE lzo,
    needofimprovement character varying(765) ENCODE lzo,
    productofinterest character varying(765) ENCODE lzo,
    externaltags character varying(765) ENCODE lzo,
    flowusers character varying(765) ENCODE lzo,
    systemofinterest character varying(765) ENCODE lzo,
    unionid character varying(65535) ENCODE lzo,
    hospitaltype character varying(255) ENCODE lzo,
	etl_create_time character varying(200) ENCODE lzo,
    etl_update_time character varying(200) ENCODE lzo,
    etl_create_by character varying(200) ENCODE lzo,
    etl_update_by character varying(200) ENCODE lzo
) DISTSTYLE AUTO;
"""
parsed_ddl = parse_ddl(aaa)
target_schema = parsed_ddl['schema']
target_table = parsed_ddl['table']
columns = parsed_ddl['columns']
truncate_statement = f"TRUNCATE TABLE {target_schema}.{target_table};"
source_schema = ''
column_names = ',\n    '.join([col['name'] for col in columns])
insert_statement = (
    f"INSERT INTO {target_schema}.{target_table} (\n"
    f"    {column_names}\n"
    f")\n"
    f"SELECT\n"
    f"    {column_names}\n"
    f"FROM {source_schema};"
)

print(target_schema,target_table,columns)
print(truncate_statement)
print(column_names)
print(insert_statement)