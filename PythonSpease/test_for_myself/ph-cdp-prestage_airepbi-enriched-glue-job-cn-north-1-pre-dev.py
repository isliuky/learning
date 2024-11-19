"""
Common enriched layer data process glue job, get load_id from lambda and fetch data from raw layer then process it into
enriched layer.
"""

import datetime
import sys
import uuid

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from boto3.dynamodb.conditions import Key
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, udf, lit, sha2, concat
from pyspark.sql.types import StringType
from bayer_cdp_common_utils.client_generator import _logger
from bayer_cdp_common_utils.conf import ConfigGlobal
from bayer_cdp_common_utils.dynamo_handler import create_or_update_item_func
from bayer_cdp_common_utils.glue_etl_handler import get_df_from_s3, get_job_run_id
from bayer_cdp_common_utils.s3_handler import get_load_id_func
from bayer_cdp_common_utils import data_quality


def raw_to_enriched_etl_process():
    job_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    local_logger.info("*********************Start Glue Enriched Layer ETL Job**********************")
    job_run_id = get_job_run_id(job_name)
    dynamo_config_tb = dynamodb_resource.Table(ConfigGlobal.dynamo_conf_tb_name)  # get configuration from dynamodb
    entity_config = dynamo_config_tb.query(
        KeyConditionExpression=Key("domain").eq(domain) & Key("entity").eq(entity)).get("Items")[0]
    primary_keys = entity_config.get("primary_keys")
    source_system = entity_config.get('source_system')
    country_code = entity_config.get('country_code', "CN")
    raw_s3_path = f"s3://{s3_raw_bucket}/{domain}/{entity}/load_id={load_id}"
    process_info = dict()
    try:
        raw_df = get_df_from_s3(spark, "parquet", raw_s3_path)
        process_info["row_cnt"] = raw_df.count()
        raw_df = raw_df.withColumn("load_id", get_load_id_udf(input_file_name()))
        # data quality check, filter invalid data
        raw_df = data_quality.run(
            spark,
            raw_df,
            rules=entity_config.get("data_quality_rules", dict()),
            quarantine_path=s3_enriched_quarantine_path,
        )
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
        enriched_df = raw_df.withColumn('sys_record_id', uuid_udf())
        if primary_keys:
            pk_list = primary_keys.split(",")
            enriched_df = enriched_df.withColumn('sys_primary_key', sha2(concat(*pk_list), 256))
        else:
            enriched_df = enriched_df.withColumn('sys_primary_key', lit("null"))
        enriched_df = enriched_df.withColumn('sys_created_dt', lit(job_start_time))
        enriched_df = enriched_df.withColumn('sys_created_by', lit(job_name))
        enriched_df = enriched_df.withColumn('sys_created_id', lit(job_run_id))
        enriched_df = enriched_df.withColumn('sys_data_source', lit(source_system + '_' + country_code))
        enriched_df = enriched_df.withColumn('sys_tenant', lit(country_code))
        enriched_df = enriched_df.withColumn('sys_delete_flag', lit("0"))
        enriched_df = enriched_df.withColumn('sys_src_modified_dt', lit(job_start_time))
        enriched_df = enriched_df.withColumn("load_id", get_load_id_udf(input_file_name()))
        enriched_df.repartition("load_id").write.partitionBy("load_id").parquet(s3_enriched_path, mode="overwrite")
        process_info["status"] = "succeeded"
    except Exception as e:
        local_logger.error(f"***********Glue job {domain}.{entity} of load_id {load_id} failed due to: {e}**********")
        process_info["status"] = "failed"
        process_info["job_error_msg"] = str(e)
        raise e
    finally:
        job_end_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        job_status = process_info.get("status")
        log_item = dict(zip(ConfigGlobal.dynamo_log_tb_schema, [
            f"{domain}-{entity}-{layer}-{job_status}",
            "-".join([load_id, job_run_id]),
            domain, entity, job_start_time, job_end_time, job_run_id, layer, load_id, process_info.get("row_cnt"),
            process_info.get("job_error_msg")
        ]))
        create_or_update_item_func(ConfigGlobal.dynamo_log_tb_name, log_item)
        local_logger.info("*********************Glue ETL Job End**********************")


if __name__ == '__main__':
    local_logger = _logger()
    args = getResolvedOptions(sys.argv, ['JOB_NAME', "DOMAIN", 'ENTITY', "ENV", "LOAD_ID", 'REGION'])
    job_name = args['JOB_NAME']
    domain = args['DOMAIN']
    entity = args['ENTITY']
    env = args['ENV']
    load_id = args['LOAD_ID']
    region = args['REGION']
    layer = "enriched"
    dynamodb_resource = boto3.resource('dynamodb', region_name=region)
    glue_client = boto3.client('glue', region_name=region)
    s3_raw_bucket = f"ph-cdp-raw-{env}-{region}"
    s3_enriched_bucket = f"ph-cdp-enriched-{env}-{region}"
    s3_enriched_path = f"s3://{s3_enriched_bucket}/{domain}/{entity}"
    s3_enriched_quarantine_path = f"s3://{s3_enriched_bucket}/quarantine/{domain}/{entity}"

    # init glue spark session with configuration
    spark_session = SparkSession.builder.config("spark.sql.files.ignoreCorruptFiles", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.autoBroadcastJoinThreshold", 20971520).getOrCreate()
    sc = spark_session.sparkContext
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(job_name, args)

    # registration of udf functions in spark
    get_load_id_udf = udf(get_load_id_func)
    try:
        raw_to_enriched_etl_process()
    except Exception:
        local_logger.exception("Mark job as failed!")
        raise
    finally:
        spark.stop()
        job.commit()
