"""
Common landing job  for bayer-cdp glue etl pipeline: pull data from different source like api, s3, sftp, relationship
database etc...
"""
import datetime
import re
from functools import partial

from bayer_cdp_common_utils.client_generator import _client
from bayer_cdp_common_utils.conf import ConfigGlobal
from bayer_cdp_common_utils.dynamo_handler import get_entity_config
from bayer_cdp_common_utils.glue_argparser import get_glue_args
from bayer_cdp_common_utils.glue_etl_handler import get_job_run_id
from bayer_cdp_common_utils.redshift_handler import redshift_insert_func, redshift_query_executor
from bayer_cdp_common_utils.s3_handler import s3_copy_func
from bayer_cdp_common_utils.secret_manager_handler import get_secret
from bayer_cdp_common_utils.api_handler import get_api_token, get_auth_info, api_load_to_s3
from pytz import timezone

from bayer_cdp_common_utils.salesforce_handler.mtm_integration import (
    MtmJob,
    add_mtm_jobs_log,
    mtm_job_monitor,
    DataSource as mtm_ds,
)
from bayer_cdp_common_utils.salesforce_handler import bulk_extract as sf_bulk_extract
from bayer_cdp_common_utils.salesforce_handler import connect_salesforce, cdp_job_monitor


def get_df_from_rdms(spark, query: str, sys_name: str):
    db_username = conn_info.get('username')
    db_password = conn_info.get('password')
    port = conn_info.get('port')
    server = conn_info.get('server')

    if sys_name == "mssql":
        return spark.read \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .options(
            driver='com.microsoft.sqlserver.jdbc.SQLServerDriver',
            url=f"jdbc:sqlserver://{server}:{port};databaseName={db_name};",
            query=query,
            user=db_username,
            password=db_password
        ).load()

    elif sys_name == "mysql":
        return spark.read \
            .format("jdbc") \
            .options(
            driver='com.mysql.cj.jdbc.Driver',
            url=f"jdbc:mysql://{server}:{port}/{db_name}",
            query=query,
            user=db_username,
            password=db_password
        ).load()
    elif sys_name == "pgsql":
        return spark.read \
            .format("jdbc") \
            .options(
            driver='org.postgresql.Driver',
            url=f"jdbc:postgresql://{server}:{port}/{db_name}",
            query=query,
            user=db_username,
            password=db_password
        ).load()


def rdms_source_landing_func(spark):
    # default output format: parquet
    landing_path = f"s3://{landing_bucket}/{domain}/{load_id}/{entity}/"
    load_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    load_mode = entity_config.get("load_mode", "full")
    inc_filter = ""
    offset_col = entity_config.get('incremental_load_col')
    if load_mode == "incremental":
        assert offset_col, f"Incremental load mode must have 'incremental_load_col' in " \
                           f"configuration: {domain}.{entity}"

        query_max_offset = f"select max(load_offset) from " \
                           f"{ConfigGlobal.redshift_log_schema}.{ConfigGlobal.redshift_incremental_load_log_table}" \
                           f" where src_database = '{db_name}' and src_table = '{entity}' "
        query_max_offset_res = redshift_query_executor(ConfigGlobal.redshift_cluster_id,
                                                       ConfigGlobal.redshift_db_nm,
                                                       query_max_offset)
        max_offset = query_max_offset_res[0][0].get("stringValue")
        if max_offset is not None:
            inc_filter = f"where {offset_col} > {max_offset}"

    elif load_mode == "customized":
        assert entity_config.get('customized_load_sql'), f"Incremental load mode must have 'customized_load_sql' in " \
                                                         f"configuration: {domain}.{entity}"
    query_dict = dict(
        zip(
            ["full", "incremental", "customized"],
            [f"select * from {schema}.{entity}" if schema else f"select * from {entity}",
             f"select * from {schema}.{entity} {inc_filter}" if schema else f"select * from {entity} {inc_filter}",
             entity_config.get("customized_load_sql", '')
             ]
        )
    )
    df = get_df_from_rdms(spark, query=query_dict[load_mode], sys_name=system_nm)
    df = df.select([col(c).cast("string") for c in df.columns])
    df.repartition(int(entity_config.get("landing_load_partitions", 10))).write.parquet(landing_path, mode="overwrite")
    load_end_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    if load_mode == "incremental":
        lz_df = spark.read.parquet(landing_path)
        load_row_cnt = lz_df.count()
        max_record = lz_df.groupby().agg(max_(offset_col)).select(f"max({offset_col})").collect()[0][0]
        load_incremental_catalog_dict = dict(zip(
            ConfigGlobal.rdms_load_catalog_cols, ["Microsoft SQLServer", domain, entity, "AWS S3", landing_path,
                                                  job_name, job_run_id, load_start_time, load_end_time,
                                                  load_row_cnt, max_record]
        ))
        redshift_insert_func(
            ConfigGlobal.redshift_cluster_id,
            ConfigGlobal.redshift_db_nm,
            ConfigGlobal.redshift_log_schema,
            ConfigGlobal.api_load_catalog_cols,
            [load_incremental_catalog_dict]
        )


def s3_source_landing_func():
    source_bucket = f"ph-cdp-nprod-{env}-{region}" if env != "prod" else f"ph-cdp-{env}-{region}"
    source_prefix = entity_config.get("s3_source_prefix", f"ph-cdp-sftp-inbound-{env}/{domain}/{entity}/")
    source_prefix = source_prefix if source_prefix.endswith("/") else source_prefix + "/"
    archive_prefix = f"ph-cdp-sftp-inbound-{env}/archive/{domain}/{entity}/{cn_date}/"
    key_items = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
    suffix = entity_config["landing_file_format"]
    replace_file_name = entity_config.get("replace_file_name", "").replace('{cn_date}', cn_date)
    sr_file_pattern = entity_config.get("source_file_pattern", f"{entity}_(\d+_)?{cn_date}").replace('{cn_date}',
                                                                                                     cn_date)
    data_file_ptn = rf"{source_prefix}{sr_file_pattern}.{suffix}"  # use re to filter valid source file
    signal_file_ptn = rf"{source_prefix}{sr_file_pattern}.ok"  # use re to filter valid signal file
    data_keys = [key.get("Key") for key in key_items.get("Contents") if re.match(data_file_ptn, key.get("Key"))]
    signal_keys = [key.get("Key") for key in key_items.get("Contents") if re.match(signal_file_ptn, key.get("Key"))]

    if replace_file_name:
        assert data_keys.__len__() == 1, "Found multi source file while replace_file_name is needed, " \
                                         "please check configuration"
        for rep_file in replace_file_name.split(";"):
            s3_copy_func(source_bucket,
                         data_keys[0],
                         source_bucket,
                         source_prefix + rep_file + "." + suffix)

    for data_key in data_keys:
        file_nm = data_key.split("/")[-1]
        target_key = f"{domain}/{load_id}/{entity}/{file_nm}"
        s3_copy_func(source_bucket, data_key, landing_bucket, target_key)  # copy source file to landing layer
        s3_copy_func(source_bucket, data_key, source_bucket, archive_prefix + file_nm)  # archive data

    s3_client.delete_objects(  # delete files in source prefix
        Bucket=source_bucket,
        Delete={
            "Objects": [{"Key": key} for key in [*data_keys, *signal_keys]]
        }
    )


def get_token_by_url(token_params, url, conn_info, proxies):
    for k, v in token_params.items():
        if not v:
            token_params[k] = conn_info.get(k)
    token_params_list = [f"{k}={v}" for k, v in token_params.items()]
    get_token_url = url + "?" + "&".join(token_params_list)
    token = get_api_token(get_token_url, proxies)
    return token


def api_source_landing_func():
    api_url_list = entity_config['api_url'].split(',')
    for api_url in api_url_list:
        # data_url_list, headers, post_body_type, post_contents, proxies, api_source_conf, base_url, post_offset
        request_params = get_auth_info(domain, entity, api_url)
        api_landing_s3 = f"s3://{landing_bucket}/{domain}/{load_id}/{entity}/{entity}"
        post_contents = request_params[3]
        for data_url in request_params[0]:
            load_resp = api_load_to_s3(domain=domain, entity=entity,
                                       s3_path=api_landing_s3, url=data_url, api_url=api_url,
                                       headers=request_params[1],
                                       post_body_type=request_params[2], post_body=post_contents,
                                       proxies=request_params[4],
                                       entity_config=entity_config,
                                       api_conf=request_params[5])
            load_end_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            # write load log
            load_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            post_contents = str(post_contents).replace("'", '')
            load_catalog_dict = dict(zip(
                ConfigGlobal.api_load_catalog_cols,
                [request_params[6], api_url, load_start_time, load_end_time, post_contents,
                 job_name, job_run_id, "s3", load_resp[0], load_resp[1], request_params[7]]
            ))
            redshift_insert_func(
                ConfigGlobal.redshift_cluster_id,
                ConfigGlobal.redshift_db_nm,
                ConfigGlobal.redshift_log_schema,
                ConfigGlobal.redshift_api_load_log_table,
                [load_catalog_dict]
            )


def salesfore_source_landing_func():
    secret_name = f"phcdp/salesforce/{sf_identifier}"
    landing_path = f"s3://{landing_bucket}/{domain}/{load_id}/{entity}/"
    sf = connect_salesforce(secret_name)
    job = MtmJob(sf_name, sf, load_id)
    if job.source == mtm_ds.SF and job.target == mtm_ds.CDP:
        callback = partial(add_mtm_jobs_log, job)
        ctx = {
            "PROVIDER": "MTM",
            "LOAD_ID": load_id,
            "CDP_JOB_RUN": job_run_id,
            "CDP_JOB_TYPE": "Glue",
            "CDP_JOB_ERROR": "",
            "SF_JOB_RUN": job.id,
            "SF_NAME": job.name,
            "SF_JOB_TYPE": job.type,
            "SF_API_OBJECT": job.object_name,
            "SF_SQLQUERY": job.sql_query,
            "SOURCE": sf.sf_instance,
            "TARGET": landing_path,
            "SF_INGEST_ERROR": "",
        }
        with mtm_job_monitor(job), cdp_job_monitor(ctx):
            sf_bulk_extract(
                sf,
                object_name=job.object_name,
                query=job.sql_query,
                s3_path=landing_path,
                callback=callback,
            )
    else:
        raise ValueError(f"Invalid job source and target, {job.source} -> {job.target}")


if __name__ == '__main__':
    args = get_glue_args(
        positional=[
            "JOB_NAME",
            "DOMAIN",
            "ENTITY",
            "LOAD_ID"
        ],
        optional={
            "REGION": ConfigGlobal.region,
            "ENV": ConfigGlobal.env
        },
    )
    job_name = args['JOB_NAME']
    job_run_id = get_job_run_id(job_name, args)
    domain = args['DOMAIN']
    entity = args['ENTITY']
    env = args['ENV']
    region = args['REGION']
    load_id = args["LOAD_ID"]

    tz_local = timezone("Asia/Shanghai")
    cn_date = datetime.datetime.now(tz=tz_local).strftime("%Y%m%d")
    s3_client = _client("s3")
    landing_bucket = f"ph-cdp-landing-{env}-{region}"
    entity_config = get_entity_config(domain, entity)
    source_system = entity_config['source_system']
    system_nm = source_system.split("_")[0]

    if system_nm == "sftp":
        s3_source_landing_func()
    elif system_nm in ("mssql", "mysql", "pgsql"):
        from pyspark.sql.functions import max as max_, col
        from pyspark.sql import SparkSession

        db_name = entity_config.get("src_database") if entity_config.get("src_database") \
            else source_system.split(f"{system_nm}_")[-1]
        conn_id = f"phcdp/{system_nm}/" + db_name if not entity_config.get("conn_id") else entity_config.get("conn_id")
        conn_info = get_secret(conn_id)[1]
        schema = entity_config.get("src_schema", "")
        spark_session = SparkSession.builder \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        rdms_source_landing_func(spark_session)
    elif system_nm == "api":
        api_source_landing_func()
    elif system_nm == "salesforce":
        sf_identifier = entity_config["salesforce_identifier"]
        sf_name = entity_config["salesforce_name"]
        salesfore_source_landing_func()
    else:
        raise SystemError(f"Unsupported system found: {source_system}")
