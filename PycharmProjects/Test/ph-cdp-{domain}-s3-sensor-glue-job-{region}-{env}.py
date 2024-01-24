import datetime
import re
import time

import boto3
from bayer_cdp_common_utils.glue_etl_handler import get_job_run_id
from bayer_cdp_common_utils.dynamo_handler import get_entity_config, update_sfn_broker_tb
from bayer_cdp_common_utils.glue_argparser import get_glue_args
from bayer_cdp_common_utils.conf import ConfigGlobal

from pytz import timezone


def s3_object_sensor():
    s3_client = boto3.client("s3", region_name=region)
    key_items = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=source_prefix)
    is_signal_file = entity_config.get("is_signal_file")
    sr_file_pattern = entity_config.get("source_file_pattern", f"{entity}_(\d+_)?{cn_date}")
    data_pattern = sr_file_pattern.replace('{cn_date}', cn_date)
    data_file_ptn = rf"{source_prefix}{data_pattern}.{suffix}"  # use re to filter valid source file
    signal_file_ptn = rf"{source_prefix}{data_pattern}.ok"  # use re to filter valid signal file
    data_keys = [key.get("Key") for key in key_items.get("Contents", []) if re.match(data_file_ptn, key.get("Key"))]
    signal_keys = [key.get("Key") for key in key_items.get("Contents", []) if re.match(signal_file_ptn, key.get("Key"))]
    return False if not data_keys or data_keys and not signal_keys and is_signal_file.upper() == 'TRUE' else True


def mysql_sensor():
    pass


def mssql_sensor():
    pass


def pgsql_sensor():
    pass


def api_sensor():
    pass


if __name__ == '__main__':

    args = get_glue_args(
        positional=[
            "JOB_NAME",
            "DOMAIN",
            "ENTITY",
        ],
        optional={
            "REGION": ConfigGlobal.region,
            "ENV": ConfigGlobal.env,
            "STATE_MACHINE_NAME": "",
            "EXECUTION_NAME": ""
        },
    )
    job_name = args['JOB_NAME']
    job_run_id = get_job_run_id(job_name=job_name, job_arguments=args)
    domain = args['DOMAIN']
    entity = args['ENTITY']
    env = args['ENV']
    region = args['REGION']
    state_machine_name = args['STATE_MACHINE_NAME']
    execution_name = args['EXECUTION_NAME']
    tz_local = timezone("Asia/Shanghai")
    cn_date = datetime.datetime.now(tz=tz_local).strftime("%Y%m%d")
    entity_config = get_entity_config(domain, entity)

    if "sftp" in entity_config['source_system']:

        suffix = entity_config.get("landing_file_format")
        source_sensor_retry = int(entity_config.get("source_sensor_retry_time", 1))
        poke_interval = int(entity_config.get("source_sensor_poke_interval", 60))
        s3_bucket = f"ph-cdp-nprod-{env}-{region}" if env != "prod" else f"ph-cdp-{env}-{region}"
        source_prefix = entity_config.get("s3_source_prefix", f"ph-cdp-sftp-inbound-{env}/{domain}/{entity}/")
        source_prefix = source_prefix if source_prefix.endswith("/") else source_prefix + "/"
        is_met = s3_object_sensor()
        is_soft_fail = entity_config.get("is_soft_fail", "false")
        current_utc_now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")

        while source_sensor_retry > 0 and not is_met:
            time.sleep(poke_interval)
            source_sensor_retry -= 1
            is_met = s3_object_sensor()

        if not is_met:
            if is_soft_fail.lower() == "false":
                raise Exception(f"S3 Sensor glue job failed: "
                                f"no valid file found in s3://{s3_bucket}/{source_prefix}")
            elif is_soft_fail.lower() == "true":

                update_sfn_broker_tb([job_name,
                                      job_run_id,
                                      state_machine_name,
                                      execution_name,
                                      current_utc_now,
                                      "is_soft_fail_flag",
                                      "bool",
                                      {"is_soft_fail_flag": 1}
                                      ])
