import jsontest
import os.path
import sys
import tempfile
import textwrap
from functools import partial
from pathlib import Path
from typing import Callable

from bayer_cdp_common_utils.client_generator import _logger
from bayer_cdp_common_utils.conf import ConfigGlobal
from bayer_cdp_common_utils.dynamo_handler import get_entity_config
from bayer_cdp_common_utils.glue_argparser import get_glue_args, OptionValue
from bayer_cdp_common_utils.glue_etl_handler import get_job_run_id
from bayer_cdp_common_utils.redshift_handler import redshift_query_executor
from bayer_cdp_common_utils.s3_handler import (
    s3_delete_folder,
    s3_download,
    s3_upload,
)
from bayer_cdp_common_utils.salesforce_handler import (
    bulk_load,
    bulk_load_completed,
    get_unsuccessful_records,
    CsvProcessor,
    CsvValidator,
    get_object_fields,
)
from bayer_cdp_common_utils.salesforce_handler import (
    connect_salesforce,
    cdp_job_monitor,
)
from bayer_cdp_common_utils.salesforce_handler.bulk2 import (
    SFBulk2Handler,
    SalesforceBulkV2LoadError,
    SalesforceOperationError,
)
from bayer_cdp_common_utils.salesforce_handler.mtm_integration import (
    add_mtm_jobs_log,
    mtm_job_monitor,
    MtmJob,
    DataSource as mtm_ds,
)
from simple_salesforce import Salesforce

LOG = _logger()


def _unload_redshift_to_s3(query, s3_path, max_file_size="96 MB", parallel=True):
    s3_delete_folder(s3_path)
    exp_parallel = "ON" if parallel else "OFF"
    sql = textwrap.dedent(
        f"""
        UNLOAD ($${query}$$) 
        TO '{s3_path}' 
        IAM_ROLE '{ConfigGlobal.redshift_iam_role}' 
        ALLOWOVERWRITE 
        HEADER 
        DELIMITER ',' 
        ADDQUOTES 
        NULL AS '' 
        EXTENSION 'csv' 
        PARALLEL {exp_parallel} 
    """
    )
    if max_file_size:
        sql += f" MAXFILESIZE {max_file_size}"

    redshift_query_executor(
        cluster_id=ConfigGlobal.redshift_cluster_id,
        db_name=ConfigGlobal.redshift_db_nm,
        sql=sql,
    )


def _make_cdp_monitor_log(job: MtmJob, **kwargs):
    provider = "MTM"
    load_id = kwargs.get("load_id") or job.last_run_batch_id
    return {
        "PROVIDER": provider,
        "LOAD_ID": load_id,
        "CDP_JOB_RUN": kwargs["cdp_job_run"],
        "CDP_JOB_TYPE": kwargs["cdp_job_type"],
        "CDP_JOB_ERROR": "",
        "SF_JOB_RUN": job.id,
        "SF_NAME": job.name,
        "SF_JOB_TYPE": job.type,
        "SF_API_OBJECT": job.object_name,
        "SF_SQLQUERY": job.sql_query,
        "SOURCE": kwargs["source"],
        "TARGET": kwargs["target"],
        "SF_INGEST_ERROR": "",
    }


def etl_s3_to_salesforce(
    sf: Salesforce,
    job: MtmJob,
    s3_path,
    concurrency=1,
    csv_transform: Callable = None,
    csv_validate: Callable = None,
    enable_monitor: bool = True,
    ctx_cdp_monitor=None,
):
    callback = partial(add_mtm_jobs_log, job)
    if csv_transform is None:
        csv_transform = partial(
            CsvProcessor.append, values={"MTM_BatchId__c": job.last_run_batch_id}
        )

    def load():
        LOG.info(f"Ingesting into salesforce: {job.object_name}")
        results = bulk_load(
            sf,
            object_name=job.object_name,
            s3_path=s3_path,
            callback=callback,
            concurrency=concurrency,
            csv_transform=csv_transform,
            csv_validate=csv_validate,
        )
        total = sum([_["numberRecordsTotal"] for _ in results])
        job.data_count = total
        if bulk_load_completed(results):
            return

        local_path = tempfile.mkdtemp()
        bulk_object = getattr(SFBulk2Handler(sf), job.object_name)
        _, sf_error = get_unsuccessful_records(bulk_object, results, local_path)
        sf_error = json.dumps(sf_error, ensure_ascii=False)
        raise SalesforceBulkV2LoadError(sf_error)

    if enable_monitor:
        ctx = ctx_cdp_monitor or _make_cdp_monitor_log(
            job, target=sf.sf_instance, source=s3_path
        )
        with cdp_job_monitor(ctx), mtm_job_monitor(job):
            try:
                load()
            except SalesforceOperationError as e:
                ctx["SF_INGEST_ERROR"] = str(e)
                raise e
    else:
        load()


def etl_redshift_to_salesforce(
    sf: Salesforce,
    job: MtmJob,
    s3_raw_path,
    concurrency=1,
    ctx_cdp_monitor=None,
    unload_parallel=True,
):
    ctx = ctx_cdp_monitor or _make_cdp_monitor_log(
        job, target=sf.sf_instance, source=ConfigGlobal.redshift_cluster_id
    )
    with cdp_job_monitor(ctx), mtm_job_monitor(job):
        LOG.info(f"Unloading redshift data to s3: {s3_raw_path}")
        # rename redshift fields name to match salesforce fields in `sql_query`
        _unload_redshift_to_s3(job.sql_query, s3_raw_path, parallel=unload_parallel)

        fields = get_object_fields(
            api_object=getattr(sf, job.object_name), return_name=False
        )
        fields_mapping = {k["name"].lower(): k["name"] for k in fields}

        def csv_transform(file):
            CsvProcessor.append(
                file, values={"MTM_BatchId__c".lower(): job.last_run_batch_id}
            )
            CsvProcessor.rename_header(file, header=fields_mapping)

        def csv_validate(file):
            CsvValidator.validate_header(file, list(fields_mapping.values()))
            CsvValidator.validate_data(file, fields)

        try:
            etl_s3_to_salesforce(
                sf,
                job,
                s3_raw_path,
                concurrency,
                csv_transform,
                csv_validate,
                enable_monitor=False,
            )
        except SalesforceOperationError as e:
            ctx["SF_INGEST_ERROR"] = str(e)
            raise e


def etl_redshift_to_sfdc_s3(
    sf: Salesforce,
    job: MtmJob,
    s3_raw_path,
    ctx_cdp_monitor=None,
):
    ctx = ctx_cdp_monitor or _make_cdp_monitor_log(
        job, target=sf.sf_instance, source=ConfigGlobal.redshift_cluster_id
    )
    with cdp_job_monitor(ctx), mtm_job_monitor(job):
        LOG.info(f"Unloading redshift data to s3: {s3_raw_path}")
        # rename redshift fields name to match salesforce fields in `sql_query`
        _unload_redshift_to_s3(job.sql_query, s3_raw_path, max_file_size="1 GB")

        def csv_transform(file):
            CsvProcessor.append(
                file, values={"MTM_BatchId__c".lower(): job.last_run_batch_id}
            )

        try:
            local_path = tempfile.mkdtemp(prefix="sfdc_s3_")
            LOG.info(f"downloading from {s3_raw_path!r} and save into {local_path!r}")
            s3_download(s3_raw_path, local_path)
            csv_files = list(Path(local_path).glob("*.csv"))
            for csv_file in csv_files:
                LOG.info(f"transforming {str(csv_file)}")
                csv_transform(csv_file)

            # mark final file
            last_one = csv_files[-1]
            last_one.rename(str(last_one.absolute()).replace(".csv", "_final.csv"))
            s3_target_path = os.path.join(job.s3_uri, load_id)
            LOG.info(f"uploading to {s3_target_path!r}")
            s3_upload(local_path, s3_target_path)

        except SalesforceOperationError as e:
            ctx["SF_INGEST_ERROR"] = str(e)
            raise e


if __name__ == "__main__":
    args = get_glue_args(
        positional=[
            "JOB_NAME",
            "DOMAIN",
            "ENTITY",
            "LOAD_ID",
        ],
        optional={
            "REGION": ConfigGlobal.region,
            "ENV": ConfigGlobal.env,
            "UNLOAD_PARALLEL": True,
            "LOAD_CONCURRENCY": 8,
        },
    )
    job_name = args["JOB_NAME"]
    job_run_id = get_job_run_id(job_name, args)
    env = args["ENV"]
    domain = args["DOMAIN"]
    entity = args["ENTITY"]
    region = args["REGION"]
    load_id = args["LOAD_ID"]
    cdp_job_type = "Glue"
    load_concurrency = OptionValue.get_int(args["LOAD_CONCURRENCY"])
    unload_parallel = OptionValue.get_bool(args["UNLOAD_PARALLEL"])

    raw_bucket = f"ph-cdp-raw-{env}-{region}"
    s3_raw_path = f"s3://{raw_bucket}/salesforce_staging/{domain}/{entity}/{load_id}/"

    entity_config = get_entity_config(domain, entity)
    source_system = entity_config.get("source_system", "salesforce")
    system_nm = source_system.split("_")[0]
    assert system_nm == "salesforce", f"source_system {source_system} is not salesforce"

    sf_identifier = entity_config["salesforce_identifier"]
    sf_name = entity_config["salesforce_name"]
    secret_name = f"phcdp/salesforce/{sf_identifier}"

    LOG.info(f"Connecting to salesforce {sf_identifier}...")
    sf = connect_salesforce(secret_name)

    LOG.info(f"Fetching job for table {sf_name!r}...")
    job = MtmJob(sf_name, sf, load_id)
    if not job.active:
        LOG.warning(f"Job {job.name!r} is not active.")
        sys.exit(0)

    if job.source == mtm_ds.CDP:
        ctx = _make_cdp_monitor_log(
            job,
            target=sf.sf_instance,
            source=ConfigGlobal.redshift_cluster_id,
            cdp_job_type=cdp_job_type,
            cdp_job_run=job_run_id,
        )
        if job.target == mtm_ds.SF:
            etl_redshift_to_salesforce(
                sf,
                job,
                s3_raw_path,
                load_concurrency,
                ctx_cdp_monitor=ctx,
                unload_parallel=unload_parallel,
            )
        elif job.target == mtm_ds.S3:
            etl_redshift_to_sfdc_s3(
                sf,
                job,
                s3_raw_path,
                ctx_cdp_monitor=ctx,
            )
        else:
            raise ValueError(f"Invalid job target: {job.target}")

    else:
        raise ValueError(f"Invalid job source: {job.source}")
