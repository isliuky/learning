"""
Common raw layer data process glue job, get load_id from lambda and fetch data from landing layer then process it into
raw layer.
"""
import datetime
import traceback
from functools import reduce

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from bayer_cdp_common_utils.client_generator import _logger
from bayer_cdp_common_utils.conf import ConfigGlobal
from bayer_cdp_common_utils.dynamo_handler import create_or_update_item_func, update_sfn_broker_tb, get_entity_config
from bayer_cdp_common_utils.glue_etl_handler import get_df_from_s3, get_job_run_id
from bayer_cdp_common_utils.s3_handler import list_s3_objs, s3_parser_to_bucket_prefix, s3_unzip_func, is_s3_path_empty
from bayer_cdp_common_utils.glue_argparser import get_glue_args
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, udf
from pyspark.sql.types import StructType


def landing_to_raw_etl_process():
    job_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    LOG.info("*********************Start RAW Glue ETL Job**********************")
    standard_columns = entity_config.get("standard_columns").split(",")
    # generate landing layer s3 file path list to process
    lz_s3_path = f"s3://{s3_lz_bucket}/{domain}/{load_id}/{entity}/"
    landing_file_format = entity_config.get("landing_file_format")
    file_in_zip_pattern = entity_config.get("file_inzip_pattern")
    is_derived_input_file_nm = entity_config.get("is_derived_input_file_name", "false").lower()
    etl_process_info = {}
    lz_objs = list_s3_objs(lz_s3_path, landing_file_format)
    lz_objs_file_name = [s3_parser_to_bucket_prefix(key)[-1] for key in lz_objs]
    source_bucket = f"ph-cdp-nprod-{env}-{region}" if env != "prod" else f"ph-cdp-{env}-{region}"
    source_prefix = entity_config.get("s3_source_prefix", f"ph-cdp-sftp-inbound-{env}/{domain}/{entity}/")
    source_prefix = source_prefix if source_prefix.endswith("/") else source_prefix + "/"
    source_uri = f"s3://{source_bucket}/{source_prefix}"
    matcher = udf(lambda x: source_uri + [key for key in lz_objs_file_name
                                          if key.split(".")[0] in x.replace("%20", " ")][0])

    try:
        if landing_file_format == 'zip':
            s3_unzip_func(lz_s3_path, file_in_zip_pattern)
            lz_s3_path = lz_s3_path + 'unzip_file/'
            landing_file_format = entity_config.get("file_inzip_suffix")

        lz_df = get_df_from_s3(spark, landing_file_format, lz_s3_path,
                               is_header=entity_config.get("is_header", 'true'),
                               delimiter=entity_config.get("delimiter"),
                               escape=entity_config.get("custom_escape"),
                               encoding=entity_config.get("src_encoding"),
                               sheet_name=entity_config.get("sheet_name"),
                               row_tag=entity_config.get("row_tag"),
                               skip_row=entity_config.get("skip_row"),
                               use_cols=entity_config.get("use_cols"),
                               excel_dtypes=entity_config.get("excel_dtypes")
                               ) if not is_s3_path_empty(lz_s3_path) else spark.createDataFrame([], StructType())
        row_cnt = lz_df.count()
        etl_process_info["row_cnt"] = row_cnt
        if row_cnt > 0:
            if entity_config.get("ori_cols_sequence"):
                ori_cols_li = entity_config.get("ori_cols_sequence").split(",")
                lz_df = lz_df.select(*ori_cols_li)

            if len(lz_df.columns) == len(standard_columns):
                # rename columns name from original cols to standard cols
                lz_df = reduce(lambda df, col_tuple: df.withColumnRenamed(col_tuple[0], col_tuple[1]),
                               zip(lz_df.columns, standard_columns), lz_df)
            else:
                raise Exception("Standard columns length should be equal with original columns: origin columns:{} "
                                "standard columns: {}".format(",".join(lz_df.columns), ",".join(standard_columns)))

            if is_derived_input_file_nm == "true":
                if "sftp" in entity_config.get("source_system", ""):
                    lz_df = lz_df.withColumn("input_file_name", matcher(input_file_name()))
            lz_df = lz_df.withColumn("load_id", regexp_extract(input_file_name(), r"/([0-9]{14})/", 1))
            LOG.info("********************* lz_df **********************")
            lz_df.show()
            lz_df.repartition("load_id").write.partitionBy("load_id").parquet(s3_raw_path, mode="overwrite")
        etl_process_info["status"] = "succeeded"
    except Exception as e:
        LOG.error(f"***********Glue job {domain}.{entity} of load_id {load_id} failed due to: {e}**********")
        LOG.error(traceback.format_exc())
        etl_process_info["status"] = "failed"
        etl_process_info["job_error_msg"] = str(e)
    log_item = dict(zip(ConfigGlobal.dynamo_log_tb_schema, [
        f"{domain}-{entity}-{layer}-{etl_process_info.get('status')}",
        "-".join([load_id, job_run_id]),
        domain, entity, job_start_time, datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        job_run_id, layer, load_id, etl_process_info.get("row_cnt", "0"),
        etl_process_info.get("job_error_msg", "")
    ]))
    create_or_update_item_func(ConfigGlobal.dynamo_log_tb_name, log_item)
    LOG.info("********************* Glue RAW ETL Job End **********************")
    return etl_process_info


if __name__ == '__main__':
    LOG = _logger()

    args = get_glue_args(
        positional=[
            "JOB_NAME",
            "DOMAIN",
            "ENTITY",
            "LOAD_ID"
        ],
        optional={
            "REGION": ConfigGlobal.region,
            "ENV": ConfigGlobal.env,
            "STATE_MACHINE_NAME": "",
            "EXECUTION_NAME": ""
        },
    )
    job_name = args['JOB_NAME']
    domain = args['DOMAIN']
    entity = args['ENTITY']
    env = args['ENV']
    region = args['REGION']
    layer = "raw"
    load_id = args["LOAD_ID"]
    state_machine_name = args["STATE_MACHINE_NAME"]
    execution_name = args["EXECUTION_NAME"]

    dynamodb_resource = boto3.resource('dynamodb', region_name=region)
    glue_client = boto3.client('glue', region_name=region)
    s3_lz_bucket = f"ph-cdp-landing-{env}-{region}"
    s3_raw_bucket = f"ph-cdp-raw-{env}-{region}"
    s3_raw_path = f"s3://{s3_raw_bucket}/{domain}/{entity}"
    # Init spark session with configuration
    spark_session = SparkSession.builder.config("spark.sql.files.ignoreCorruptFiles", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate()

    sc = spark_session.sparkContext
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(job_name, args)
    job_run_id = get_job_run_id(job_name)
    entity_config = get_entity_config(domain, entity)

    job_result = landing_to_raw_etl_process()
    soft_fail_flag = entity_config.get("is_soft_fail", "false").lower()
    if job_result.get("status") == "failed":
        raise Exception(f"Mark job as failed caused by {job_result.get('job_error_msg')}")

    if job_result.get("row_cnt") == 0:
        if soft_fail_flag == "true":
            update_sfn_broker_tb([job_name,
                                  job_run_id,
                                  state_machine_name,
                                  execution_name,
                                  datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S"),
                                  "is_soft_fail_flag",
                                  "bool",
                                  {"is_soft_fail_flag": 1}
                                  ])
        else:
            raise Exception(f"Empty landing s3 path or files error. Check data source or "
                            f"set is_soft_fail = true in configuration to pass this type error")

    spark.stop()
    job.commit()
