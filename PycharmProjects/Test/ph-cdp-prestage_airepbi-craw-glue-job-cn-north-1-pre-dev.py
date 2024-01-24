import logging
import re
import sys
import time

import boto3
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

if __name__ == '__main__':
    args = getResolvedOptions(sys.argv, ['JOB_NAME', "DOMAIN", "ENV", "REGION"])
    job_name = args['JOB_NAME']
    domain = args['DOMAIN']
    env = args['ENV']
    region = args['REGION']
    glue_client = boto3.client('glue', region_name=region)
    all_crawlers_li = []
    get_crawler_retry = 10
    while get_crawler_retry > 0:
        try:
            list_crawlers = glue_client.list_crawlers()
            all_crawlers_li += list_crawlers.get("CrawlerNames")
            while list_crawlers.get("NextToken"):
                list_crawlers = glue_client.list_crawlers(NextToken=list_crawlers.get("NextToken"))
                all_crawlers_li += list_crawlers.get("CrawlerNames")
                print(f'all_crawlers_li{all_crawlers_li}')
            break
        except ClientError:
            get_crawler_retry -= 1
            time.sleep(15)
    assert get_crawler_retry > 0, f"Client Error occurred even after 10 retries."
    print(f"pipei:   ^(ph-cdp)-{domain}.*{region}-{env}")
    related_crawlers = [crawler_name for crawler_name in all_crawlers_li if
                        re.match(rf"^(ph-cdp)-{domain}.*{region}-{env}", crawler_name)]
    print(f'related_crawlers{related_crawlers}')
    start_crawler_retry = 10
    # try to start each layer's crawler
    for crawler in related_crawlers:
        logging.info(f"Trying to retrieve crawler {crawler}'s running metadata")
        crawler_response = glue_client.get_crawler(Name=crawler)
        while crawler_response['Crawler']['State'] != 'READY' and start_crawler_retry > 0:
            time.sleep(30)
            start_crawler_retry -= 1
            crawler_response = glue_client.get_crawler(Name=crawler)
        try:
            glue_client.start_crawler(Name=crawler)
        except Exception as e:
            logging.warning(f"Duplicated crawler call with {crawler}, step passed.")
