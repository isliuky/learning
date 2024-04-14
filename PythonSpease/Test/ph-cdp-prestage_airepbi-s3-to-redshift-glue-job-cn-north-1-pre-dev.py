import datetime
import re
import time
import traceback

from awsglue.context import GlueContext
from bayer_cdp_common_utils.athena_handler import sense_athena_table, athena_query_executor, \
    update_partitions, athena_result_format
from bayer_cdp_common_utils.client_generator import _logger
from bayer_cdp_common_utils.conf import ConfigGlobal
from bayer_cdp_common_utils.dynamo_handler import get_entity_config
from bayer_cdp_common_utils.glue_argparser import get_glue_args
from bayer_cdp_common_utils.glue_etl_handler import get_job_run_id
from bayer_cdp_common_utils.redshift_handler import (
    sense_table_existence,
    create_redshift_stage_tb,
    redshift_query_executor,
    redshift_truncate_table,
    redshift_insert_func,
    generate_copy_manifest,
    get_rs_tb_columns,
    redshift_drop_table
)
from bayer_cdp_common_utils.s3_handler import s3_parser_to_bucket_prefix, get_incremental_prefix, \
    get_load_id_func
from botocore.exceptions import WaiterError
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as max_, regexp_extract, input_file_name


def get_mapping_conf(src_domain, src_entity):
    cfg_sql = f"select source_column_name, " \
              f"target_column_name ," \
              f"target_data_type, " \
              f"target_schema, " \
              f"target_table, " \
              f"is_active, " \
              f"derive_desc " \
              f"from metadata.s3_to_rs_mapping where source_domain = '{src_domain}' " \
              f"and source_table = '{src_entity}' "
    LOG.info(f"cfg_sql:{cfg_sql}")
    return redshift_query_executor(cluster_id, rs_db, cfg_sql)


def get_offset(target_schema, target_table):
    query_max_partition = f"""
       select 
       max(load_id) 
       from metadata.{ConfigGlobal.redshift_data_load_log_table} 
       where load_table_schema = '{target_schema}' 
       and load_table_name = '{target_table}'
       and status = 'success'
       """
    return redshift_query_executor(cluster_id, rs_db, query_max_partition)[0][0].get("stringValue")


def create_rs_ext_schema(ext_schema_nm, ext_src_db, ext_data_type="data catalog"):
    schema_ddl = f"""
    CREATE EXTERNAL SCHEMA IF NOT EXISTS {ext_schema_nm}
    FROM {ext_data_type}
    DATABASE '{ext_src_db}'
    IAM_ROLE '{rs_iam_role}';
    """
    redshift_query_executor(cluster_id, rs_db, schema_ddl)


def create_catalog_table(s3_uri, catalog_table_name, catalog_db, origin_cols):
    athena_connections = {
        "Database": catalog_db,
        "Catalog": "AwsDataCatalog"
    }

    cols_expr = ",".join([f"`{col}` STRING" for col in origin_cols])
    athena_ddl = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {catalog_table_name}
                               ({cols_expr})"""
    partition_expr = f"PARTITIONED BY (`{partition_key}` string) "
    ddl_suffix = f"""
                   ROW FORMAT SERDE 
                     'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
                   STORED AS INPUTFORMAT 
                     'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
                   OUTPUTFORMAT 
                     'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                   LOCATION
                     '{s3_uri}'"""
    athena_ddl = athena_ddl + partition_expr + ddl_suffix \
        if partition_key.lower().strip() != 'none' else athena_ddl + ddl_suffix
    athena_query_executor(athena_ddl, athena_connections, athena_result_config)


def _loader(load_type, s3_uri, rs_schema, rs_table):
    max_partition = get_offset(rs_schema, rs_table)
    bucket, prefix, _ = s3_parser_to_bucket_prefix(s3_uri)
    data_load_log_dict = dict()
    if load_type == "COPY_FROM_S3":
        cp_cmd = f"copy {rs_schema}.{rs_table} from '{s3_uri}' iam_role '{rs_iam_role}'" \
                 f"format as parquet acceptinvchars fillrecord"
        df = spark.read.option("mergeSchema", "true").parquet(s3_uri)
        load_s3_uri = s3_uri
        if max_partition and partition_key.lower().strip() != "none":
            if re.match(r"(\d){14}$", load_id):
                load_s3_uri = s3_uri + f"{partition_key}={load_id}"
                cp_cmd = f"copy {rs_schema}.{rs_table} from '{load_s3_uri}' iam_role '{rs_iam_role}' " \
                         f"format as parquet acceptinvchars fillrecord"
                df = spark.read.option("mergeSchema", "true").parquet(load_s3_uri)
            else:
                load_data_list = [f"s3://{bucket}/{p}" for p in get_incremental_prefix(bucket, prefix, max_partition)]
                if load_data_list.__len__() == 0:
                    LOG.warning(f"No incremental partition found of table {rs_schema}.{rs_table}.")
                    return
                else:
                    if is_lasted_only.lower() == "false":
                        generate_copy_manifest(load_data_list, f"{domain}_{entity}.manifest")
                        cp_cmd = f"""copy "{rs_schema}"."{rs_table}" 
                            from '{ConfigGlobal.redshift_copy_manifest_s3}{domain}_{entity}.manifest' 
                            iam_role '{rs_iam_role}' format as parquet manifest acceptinvchars fillrecord; """
                        load_s3_uri = ",".join(load_data_list)
                        df = spark.read.option("mergeSchema", "true").parquet(*load_data_list)
                    else:
                        latest_load_path = load_data_list[-1]
                        cp_cmd = f"""copy "{rs_schema}"."{rs_table}" from '{latest_load_path}' 
                                         iam_role '{rs_iam_role}' format as parquet acceptinvchars fillrecord"""
                        load_s3_uri = latest_load_path
                        df = spark.read.option("mergeSchema", "true").parquet(latest_load_path)
        if partition_key.lower().strip() != 'none':
            df = df.withColumn(partition_key, regexp_extract(input_file_name(), r'.*=((\d){14})/.*', 1))
            max_load_id = df.groupby().agg(max_(partition_key)).select(f"max({partition_key})").collect()[0][0]
        else:
            max_load_id = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")

        data_load_log_dict = dict(zip(ConfigGlobal.redshift_data_load_log_cols,
                                      [max_load_id,
                                       load_s3_uri,
                                       df.count(),
                                       cp_cmd.replace("'", ""),
                                       "success",
                                       rs_schema,
                                       rs_table,
                                       datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), ""]))

        try:
            redshift_query_executor(cluster_id, rs_db, cp_cmd, attempts=30)
        except WaiterError as waiter_e:
            query_error_msg = waiter_e.last_response.get("Error")
            if '15007' in query_error_msg and 'Table: 65535' in query_error_msg:
                LOG.error(
                    "Reached redshift varchar(max) type limitation, please change load type to 'INSERT_FROM_EXT'.")
            else:
                LOG.error(f"Error occurred while copy data from s3: {s3_uri} to table "
                          f"{rs_schema}.{rs_table}: \n  {query_error_msg}")
            data_load_log_dict["status"] = "failed"
        data_load_log_dict["load_end_time"] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        redshift_insert_func(cluster_id, rs_db,
                             ConfigGlobal.redshift_log_schema,
                             ConfigGlobal.redshift_data_load_log_table,
                             [data_load_log_dict])

    elif load_type == "INSERT_FROM_EXT":

        domain_layer = re.match(r"ph-cdp-(.*)" + f"-{env}-{region}", bucket)
        catalog_db = f"{domain_layer[1]}_{domain}_cn" if domain_layer else ConfigGlobal.athena_etl_spectrum
        ext_schema_nm = f"ext_{domain_layer[1]}_{domain}"
        create_rs_ext_schema(ext_schema_nm, catalog_db)
        tb_name = entity if domain_layer else f"{domain}_{entity}"
        if not sense_athena_table(catalog_db, tb_name):
            cols_without_partition_key = [col for col in
                                          spark.read.option("mergeSchema", "true").parquet(s3_uri).columns
                                          if col != partition_key]
            create_catalog_table(s3_uri, tb_name, catalog_db, cols_without_partition_key)
        tgt_cols = ",".join(['"' + col + '"' for col in get_rs_tb_columns(cluster_id, rs_db, rs_schema, rs_table)])
        update_partitions(catalog_db, tb_name)
        insert_from_spectrum = f"""
        INSERT INTO "{rs_schema}"."{rs_table}"
        ({tgt_cols})
        SELECT {tgt_cols} 
        FROM "{ext_schema_nm}"."{tb_name}" """
        row_count_sql = f"""
        SELECT COUNT(1) AS INSERT_ROW_COUNT FROM "{catalog_db}"."{tb_name}" """
        athena_conn = {
            "Database": catalog_db,
            "Catalog": "AwsDataCatalog"
        }
        if re.match(r"(\d){14}$", load_id):
            filter_cond = f"WHERE {partition_key} = '{load_id}' "
            insert_from_spectrum += filter_cond
            row_count_sql += filter_cond
            max_load_id = load_id
        else:
            if max_partition and partition_key.lower().strip() != "none":
                filter_cond = f"WHERE {partition_key} > '{max_partition}' "
                insert_from_spectrum += filter_cond
                row_count_sql += filter_cond

            if partition_key.lower().strip() != 'none':

                query_max_load_id = f"SELECT max({partition_key}) AS MAX_ID FROM {catalog_db}.{tb_name}"
                max_load_id = athena_query_executor(query_max_load_id, athena_conn, athena_result_config)
                assert max_load_id.__len__() > 0, f"Return None for Athena sql: {query_max_load_id}! "
                max_load_id = athena_result_format(max_load_id)[0].get("MAX_ID")

            else:
                max_load_id = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")

        data_load_log_dict = dict(zip(ConfigGlobal.redshift_data_load_log_cols,
                                      [max_load_id,
                                       f"Aws CataLog: {catalog_db}.{tb_name}",
                                       0,
                                       insert_from_spectrum.replace("'", ""),
                                       "success",
                                       rs_schema,
                                       rs_table,
                                       datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), ""]))
        # Add retry to prevent crawler
        insert_retry = 3
        insert_interval = 60
        while insert_retry > 0:
            try:
                redshift_query_executor(cluster_id, rs_db, insert_from_spectrum, attempts=30)
                break
            except WaiterError as waiter_e:
                query_error_msg = waiter_e.last_response.get("Error")
                LOG.error(f"Error occurred while insert data from external schema {ext_schema_nm} to table "
                          f"{rs_schema}.{rs_table}: \n  {query_error_msg}")
                time.sleep(insert_interval)
                insert_retry -= 1
            finally:
                if insert_retry == 0:
                    data_load_log_dict["status"] = "failed"
                data_load_log_dict["load_end_time"] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        load_row_cnt = athena_query_executor(row_count_sql, athena_conn, athena_result_config)
        assert load_row_cnt.__len__() > 0, f"Return None for Athena sql: {row_count_sql}! "
        load_row_cnt = athena_result_format(load_row_cnt)[0].get("INSERT_ROW_COUNT")
        data_load_log_dict["row_count"] = load_row_cnt
        redshift_insert_func(cluster_id, rs_db,
                             ConfigGlobal.redshift_log_schema,
                             ConfigGlobal.redshift_data_load_log_table,
                             [data_load_log_dict])

    return data_load_log_dict


def s3_to_redshift_with_mapping(mapping_conf, src_s3_uri):
    stage_table = f"{domain}_{entity}"
    stage_schema = ConfigGlobal.redshift_stage_schema
    src_df = spark.read.option("mergeSchema", "true").parquet(src_s3_uri)
    source_cols = [col for col in src_df.columns if col != partition_key]
    target_schema = mapping_conf[0][3].get("stringValue")
    target_table = mapping_conf[0][4].get("stringValue")

    is_stage_table_exist = sense_table_existence(cluster_id, rs_db, stage_schema, stage_table)
    is_target_table_exist = sense_table_existence(cluster_id, rs_db, target_schema, target_table)
    stage_tb_cols = get_rs_tb_columns(cluster_id, rs_db, stage_schema, stage_table)


    if not is_stage_table_exist:
        create_redshift_stage_tb(cluster_id, source_cols, rs_db, stage_table)
    else:
        if not sorted(source_cols) == sorted(stage_tb_cols):
            redshift_drop_table(cluster_id, rs_db, stage_schema, stage_table)
            create_redshift_stage_tb(cluster_id, source_cols, rs_db, stage_table)

    redshift_truncate_table(
        cluster_id=cluster_id,
        rs_db=rs_db,
        rs_schema=stage_schema,
        rs_tb=stage_table
    )
    # Load data from s3 into stage table
    load_result = _loader(sync_type, src_s3_uri, stage_schema, stage_table)
    assert load_result["status"] == "success", f"Data load failed during data sync to redshift stage table " \
                                               f"{stage_schema}.{stage_table}"
    target_conf_cols_with_type = [
        (row[0].get("stringValue"),
         row[1].get("stringValue"),
         row[2].get("stringValue"),
         row[-1].get("stringValue"))
        for row in mapping_conf
        if row[-2].get("stringValue").upper() == "Y"
    ]
    extra_tgt_cols_with_type = [col_tup for col_tup in target_conf_cols_with_type if col_tup[0] not in source_cols]
    sorted_by_src_cols = [tgt_col_tup[1] for tgt_col_tup in list(
        sorted([(source_cols.index(tp[0]), tp) for tp in target_conf_cols_with_type if tp[0] in source_cols],
               key=lambda x: x[0]))]
    target_conf_cols_with_type = sorted_by_src_cols + extra_tgt_cols_with_type
    target_cols_expr = ",".join(['"' + col_tup[1] + '"' for col_tup in target_conf_cols_with_type])
    target_cols_with_type_expr = ",".join(
        ['"' + col_tup[1] + '"' + f" {col_tup[2]}" for col_tup in target_conf_cols_with_type])

    if not is_target_table_exist:

        target_table_ddl = f'CREATE TABLE IF NOT EXISTS "{target_schema}"."{target_table}"' \
                           f' ({target_cols_with_type_expr})'
        ddl_history_log_dict = dict(zip(ConfigGlobal.redshift_ddl_history_log_cols,
                                        [target_table, target_schema, target_table_ddl,
                                         datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "success",
                                         "s3_to_redshift_glue_job"]))
        try:
            redshift_query_executor(cluster_id, rs_db, target_table_ddl)
        except Exception:
            LOG.error(f"Error occurred while creating table {target_schema}.{target_table} "
                      f"with DDL: {target_table_ddl}")
            ddl_history_log_dict["status"] = "failed"
        # write ddl execution load log
        redshift_insert_func(cluster_id, rs_db,
                             ConfigGlobal.redshift_log_schema,
                             ConfigGlobal.redshift_ddl_history_log_table,
                             [ddl_history_log_dict])
        assert ddl_history_log_dict["status"] == "success", f"Create redshift enriched table failed"
    # post job before insert into target table
    try:
        post_job_statement = get_entity_config(domain, entity).get("redshift_enriched_post_job")
    except Exception:
        post_job_statement = {}

    if post_job_statement:
        try:
            redshift_query_executor(cluster_id, rs_db, post_job_statement, attempts=30)
        except Exception:
            LOG.error(f"Error occurred while executing redshift enriched post job failed, "
                      f"below shows statement: \n {post_job_statement}")
            raise Exception("Process failed while executing post job.")
    # insert data from stage table to target enriched table
    transfer_expr = []
    for col_tup in target_conf_cols_with_type:
        if "derive" not in col_tup[0]:
            transfer_expr.append(f"cast(" + '"' + f'{col_tup[0]}' + '"' + f"as {col_tup[2]})")
        else:
            if "select" == col_tup[-1].lower()[:6]:
                immutable_var = col_tup[-1].split("select")[-1].strip()
                transfer_expr.append(f"cast('{immutable_var}' as {col_tup[2]})")
            else:
                transfer_expr.append(f"{col_tup[-1]}")

    cast_cols_expr = ",".join(transfer_expr)
    insert_sql = f"""INSERT INTO "{target_schema}"."{target_table}" ({target_cols_expr}) SELECT {cast_cols_expr} FROM 
        "{stage_schema}"."{stage_table}"； """
    if is_truncate.lower() == "true" or partition_key.lower().strip() == "none":
        redshift_truncate_table(
            cluster_id=cluster_id,
            rs_db=rs_db,
            rs_schema=target_schema,
            rs_tb=target_table
        )
    try:
        redshift_query_executor(cluster_id, rs_db, insert_sql, attempts=30)
    except Exception:
        LOG.error(f"Error occurred while executing insert sql: {insert_sql}")
        raise Exception("Process failed while inserting stage table into enriched table.")


def s3_to_redshift_without_mapping(src_s3_uri, tgt_schema, tgt_table):
    """ common data sync step between s3 and redshift."""

    if not sense_table_existence(cluster_id, rs_db, tgt_schema, tgt_table):
        src_df = spark.read.option("mergeSchema", "true").parquet(src_s3_uri)
        cols = src_df.columns
        if is_include_par_col.lower().strip() != "true":
            cols = cols[:-1] if cols[-1] == partition_key else cols
        col_expr = [f"max(length({col})) as max_{col}" for col in cols]
        max_cnt = src_df.selectExpr(*col_expr).collect()[0]
        col_with_max_len = dict(zip(cols, [max_cnt[f"max_{col}"] for col in cols]))
        ddl_expr = list()
        for col, col_lent in col_with_max_len.items():
            col_lent = 256 if col_lent is None else int(col_lent)
            col_lent = 128 if col_lent < 64 else col_lent
            if col_lent <= 21844:
                ddl_expr.append('"' + col + '"' + " varchar(" + str(int(col_lent) * 3) + ")")
            else:
                ddl_expr.append('"' + col + '"' + " varchar(max)")

        column_list_expr = ",".join(ddl_expr)
        dynamic_ddl = f"""
                   CREATE TABLE IF NOT EXISTS 
                   "{tgt_schema}"."{tgt_table}"
                   ({column_list_expr})
                   """
        ddl_history_log_dict = dict(zip(ConfigGlobal.redshift_ddl_history_log_cols,
                                        [tgt_table,
                                         tgt_schema,
                                         dynamic_ddl,
                                         datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                                         "success",
                                         "s3_to_redshift_glue_job"]))
        try:
            redshift_query_executor(cluster_id, rs_db, dynamic_ddl)
            LOG.info(f"Target table: {tgt_table} created successfully!")
        except Exception as error_msg:
            LOG.error(f"Table {tgt_schema}.{tgt_table} creation: {dynamic_ddl} failed due to {error_msg}")
            ddl_history_log_dict["status"] = "failed"

        redshift_insert_func(cluster_id,
                             rs_db,
                             ConfigGlobal.redshift_log_schema,
                             ConfigGlobal.redshift_ddl_history_log_table,
                             [ddl_history_log_dict])

        assert ddl_history_log_dict["status"] == "success", f"Error occurred while creating table:" \
                                                            f" {tgt_schema}.{tgt_table}"
    if is_truncate.lower() == "true" or partition_key.lower().strip() == "none":
        redshift_truncate_table(
            cluster_id=cluster_id,
            rs_db=rs_db,
            rs_schema=tgt_schema,
            rs_tb=tgt_table
        )

    load_result = _loader(sync_type, src_s3_uri, tgt_schema, tgt_table)
    assert load_result["status"] == "success", f"Data load failed during data sync to redshift target table " \
                                               f"{tgt_schema}.{tgt_table}"


if __name__ == '__main__':

    spark_session = SparkSession.builder.config("spark.sql.files.ignoreCorruptFiles", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    sc = spark_session.sparkContext
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    job_run_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    args = get_glue_args(
        positional=[
            "JOB_NAME",
            "DOMAIN",
            "ENTITY"
        ],
        optional={
            "REDSHIFT_CLUSTER": ConfigGlobal.redshift_cluster_id,
            "RS_DB": ConfigGlobal.redshift_db_nm,
            "REGION": ConfigGlobal.region,
            "ENV": ConfigGlobal.env,
            "LOAD_ID": "",
            "SYNC_METHOD": "INSERT_FROM_EXT",
            "SYNC_SOURCE_URI": "",
            "SYNC_TARGET_SCHEMA": "",
            "SYNC_TARGET_TABLE": "",
            "PARTITION_KEY": "load_id",
            "IS_INCLUDE_PAR_COL": "false",
            "IS_LATEST_ONLY": "",
            "IS_TRUNCATE": ""
        },
    )

    job_name = args["JOB_NAME"]
    domain = args["DOMAIN"]
    entity = args["ENTITY"]
    load_id = args["LOAD_ID"]
    region = args["REGION"]
    cluster_id = args["REDSHIFT_CLUSTER"]
    rs_db = args["RS_DB"]
    env = args["ENV"]
    sync_type = args["SYNC_METHOD"]
    partition_key = args["PARTITION_KEY"]
    sync_tgt_schema = args.get("SYNC_TARGET_SCHEMA")
    sync_tgt_table = args.get("SYNC_TARGET_TABLE")
    task_name = f"s3_to_redshift_{domain}_{entity}"
    task_type = "glue_redshift_api"
    step_name = "s3_to_redshift_enriched"
    sync_s3_uri = args["SYNC_SOURCE_URI"]
    is_lasted_only = args["IS_LATEST_ONLY"]
    is_truncate = args["IS_TRUNCATE"]
    is_include_par_col = args["IS_INCLUDE_PAR_COL"]

    if is_include_par_col.lower() == "true":
        assert sync_type == "INSERT_FROM_EXT", "Only 'INSERT_FROM_EXT' method support add partition key to target " \
                                               "redshift table."

    # set default as s3 enriched layer
    sync_s3_uri = sync_s3_uri if sync_s3_uri else f"s3://ph-cdp-enriched-{env}-{region}/{domain}/{entity}/"
    sync_s3_uri = sync_s3_uri if sync_s3_uri.endswith("/") else sync_s3_uri + "/"
    job_run_id = get_job_run_id(job_name)
    rs_iam_role = ConfigGlobal.redshift_iam_role
    audit_log_dict = \
        dict(zip(ConfigGlobal.redshift_audit_log_cols,
                 [job_run_date,
                  job_name,
                  job_start_time, "", "success",
                  task_type,
                  task_name,
                  step_name,
                  job_run_id
                  ]))
    LOG = _logger()
    LOG.info(f"audit_log_dict:{audit_log_dict}")

    athena_result_config = {
        "OutputLocation": ConfigGlobal.athena_results_s3
    }

    try:
        map_configuration = get_mapping_conf(domain, entity)
        LOG.info(f"map_configuration:{map_configuration}")
        LOG.info(f"sync_s3_uri:{sync_s3_uri}")
        if map_configuration.__len__() > 0:
            s3_to_redshift_with_mapping(map_configuration, sync_s3_uri)
        else:
            assert sync_tgt_schema and sync_tgt_table, "Must have params: sync_tgt_schema and sync_tgt_table" \
                                                       " due to no s3-redshift mapping configuration found!"
            s3_to_redshift_without_mapping(sync_s3_uri, sync_tgt_schema, sync_tgt_table)

    except Exception as e:
        LOG.error(f"************S3_to_Redshift job failed: {domain}.{entity} due to: {e}!************")
        LOG.error(traceback.format_exc())
        audit_log_dict["status"] = "failed"
    audit_log_dict["job_end_time"] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    redshift_insert_func(cluster_id,
                         rs_db,
                         ConfigGlobal.redshift_log_schema,
                         ConfigGlobal.redshift_audit_log_table,
                         [audit_log_dict])
    assert audit_log_dict["status"] == "success", "S3 to redshift sync job failed, " \
                                                  "find more details with Glue output log & error log"
