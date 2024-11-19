from datetime import datetime, timedelta
import datetime
import ast
import logging
import sys
from functools import reduce
import requests
import time
import re
import logging
import sys
from typing import Dict
import asyncio
import boto3
from botocore.config import Config
from pytz import timezone
from interval import Interval


class APICallError(Exception):
    def __init__(self, response):
        self.resp = response


def s3_parser_to_bucket_prefix(s3_path):
    """
    parse a full s3 path to bucket and prefix
    :param s3_path:  s3 full path string
    :return: bucket, prefix
    """
    s3_pattern = "^s3://([^/]+)/(.*?([^/]+)/?)$"
    re_result = re.match(s3_pattern, s3_path)
    if re_result:
        return re_result.groups()
    else:
        LOG.error(f"Invalid s3 path found: {s3_path}")
        raise Exception("Exception: Invalid s3 path expression")


def api_load_to_s3_inc(domain, entity, s3_path, url, headers, post_body_type, post_body, proxies,
                       api_conf: dict) -> (str, int):
    bucket, prefix, _ = s3_parser_to_bucket_prefix(s3_path)
    source_path = []
    result_list = []
    download_urls = []
    source_row_count = 0
    page_no = 0
    response_conf = api_conf.get('response')
    download_res = []

    def dynamic_post(url, headers, post_body_type, post_body, proxies=None, retry=10, interval=5):
        while retry > 0:
            try:
                if not post_body:
                    resp = requests.get(url, headers=headers, proxies=proxies)
                else:
                    resp = requests.post(url, headers=headers, data=post_body, proxies=proxies) \
                        if post_body_type.lower() != "json" else \
                        requests.post(url, headers=headers, json=post_body, proxies=proxies)
                return resp
            except Exception:
                retry -= 1
                time.sleep(interval)
        raise Exception(f"Post failed of url: {url} after {retry} retries")
    while True:
        if post_body_type == 'text':
            post_body_prefix = api_conf.get("body")["prefix"]
            post_body_dict = ast.literal_eval(post_body.split(post_body_prefix)[-1])
            if page_no == 0:
                post_body_dict[api_conf.get("page_index_key")] = int(post_body_dict[api_conf.get("page_index_key")])
            else:
                post_body_dict[api_conf.get("page_index_key")] = int(post_body_dict[api_conf.get("page_index_key")]) + 1
            post_body = post_body_prefix + str(post_body_dict)
        elif post_body_type == 'json':
            if page_no > 0:
                post_body[api_conf.get("page_index_key")] = str(int(post_body['pageNum']) + 1)
        LOG.info(f'Post body is: {post_body}')
        resp = dynamic_post(url, headers, post_body_type, post_body, proxies)
        print(f'-------resp:{resp}---------')
        if not resp.ok:
            LOG.error(f"{entity} load failed due to invalid call status code found {resp.status_code}!")
            raise APICallError(resp.text)
        data_count_key = response_conf.get('data_count_key')
        response = resp.json()
        pages_data_list = response.get(response_conf.get("data_content_key"))
        output_key = f'{prefix}' + datetime.now(tz=LOCAL_ZONE).strftime("%Y%m%d%H%M%S") + ".json"
        page_no += 1
        if pages_data_list:
            keys = data_count_key.split(".")
            data_count = response.get(keys[0]).get(keys[1]) if len(keys) == 2 else response.get(keys[0])
            LOG.info(f'Data item count: {data_count}')
            if api_conf.get("download_params"):
                get_url_key = api_conf.get("download_params").get("download_key")
                LOG.info(get_url_key)
                if entity == "udeskmobileinfo":
                    for item in pages_data_list:
                        if item.get(get_url_key):
                            begin_at = datetime.strptime(item.get('beginAt', '19000101-000000'),
                                                         "%Y-%m-%d %H:%M:%S")
                            folder = f"{domain}/{entity}/{begin_at.year}/{begin_at.month}/{begin_at.day}/"
                            begin_prefix = begin_at.strftime('%Y%m%d-%H%M%S')
                            agent_name = "empty_agent_name" if not item.get("agentName") else item.get("agentName")
                            cust_nbr = item.get('customerNumber', 'null_customer_number')
                            suffix = item.get(get_url_key).split('?')[0].split('.')[-1]
                            key = folder + begin_prefix + "-" + agent_name + "-" + cust_nbr + "." + suffix
                            download_urls.append({"url": item.get(get_url_key), "key": key})
            if download_urls:
                LOG.info("Start to download files")
                loop = asyncio.get_event_loop()
                download_res = loop.run_until_complete(download_main(download_urls))
                LOG.info(f"Download complete and get execution result: {download_res}")

            if domain == 'prestage_airepbi':
                pages_data_list = wefeng_data_source_handler(entity, url, pages_data_list, domain, entity, download_res)
                pages_data_list = convert_data_type(pages_data_list)

            result_list.extend(pages_data_list)
            source_row_count += len(result_list)
            if result_list.__len__() > 10 * 1000:
                s3_client.put_object(
                    Body=json.dumps(result_list),
                    Bucket=bucket,
                    Key=output_key
                )
                source_path.append(output_key)
                result_list = []
            if result_list.__len__() == data_count:
                break
        else:
            break
    if result_list:
        output_key = f'{prefix}' + datetime.now(tz=LOCAL_ZONE).strftime("%Y%m%d%H%M%S") + ".json"
        s3_client.put_object(
            Body=json.dumps(result_list),
            Bucket=bucket,
            Key=output_key
        )
        LOG.info(f"API data file has been processed to {output_key}")
        source_path.append(output_key)

    return ",".join(source_path), source_row_count
def api_load_to_s3(domain, entity, s3_path, url, headers, post_body_type, post_body, proxies, entity_config, api_conf):
    history_load_conf = api_conf.get("history_load")
    post_list = []
    print(f'---------history_load_conf:{history_load_conf}-----------')
    if entity_config.get("is_history_load", "false").lower() == "true" and history_load_conf:
        start = history_load_conf.get("start_value")
        end = history_load_conf.get("end_value")
        print(f'--------start:{start}---------')
        print(f'-------end:{end}---------')
        if history_load_conf["step"]:
            if history_load_conf['range_type'] == "datetime":
                chunks = []
                start_dt = datetime.strptime(start, history_load_conf["datetime_format"])
                end_dt = datetime.strptime(end, history_load_conf["datetime_format"])
                while start_dt < end_dt:
                    border = start_dt + timedelta(seconds=int(history_load_conf["step"]))
                    chunks.append((
                        start_dt.strftime(history_load_conf["datetime_format"]),
                        border.strftime(history_load_conf["datetime_format"])
                    )
                    )
                    start_dt = border
                for chunk in chunks:
                    post_body.update({
                            history_load_conf["start_key"]: chunk[0],
                            history_load_conf["end_key"]: chunk[1],
                        })
                    post_list.append(
                        post_body.copy()
                    )

        else:
            post_body.update({
                history_load_conf["start_key"]: start,
                history_load_conf["end_key"]: end,
            })
            post_list.append(post_body)

    else:
        post_list.append(post_body)
        print(f'---------post_list:{post_list}-----------')

    load_result_li = []
    for post in post_list:
        print(f'---------post:{post}-----------')
        load_result = api_load_to_s3_inc(domain, entity, s3_path, url, headers, post_body_type, post, proxies, api_conf)
        load_result_li.append(load_result)
    # Only take top 10 load result to avoid too long string
    output_keys, obj_cnt = reduce(lambda x, y: (";".join([x[0], y[0]]), x[1] + y[1]), load_result_li[:10])
    return output_keys[:65000], obj_cnt



def wefeng_data_source_handler(api_name, url, data, domain, entity, download_res):
    data_config = weFeng[api_name]
    # 获取动态映射配置
    dynamic_mapping = data_config['dynamic_mapping']

    # 获取静态映射配置
    static_mapping = data_config['static_mapping']

    # 获取api与结果表映射配置
    api_table_mapping = data_config['api_table_mapping']

    # 获取默认值列
    default_mapping = data_config['default_mapping']

    res_list = []
    if data:
        for data_dict in data:
            # 处理动态映射
            dynamic_mapping_dict = {}
            if dynamic_mapping:
                dynamic_mapping_dict = dynamic_mapping_handler(data_dict,
                                                               dynamic_mapping['original_col'],
                                                               dynamic_mapping['original_values'],
                                                               dynamic_mapping['new_add_col'])

            # 处理静态映射
            static_mapping_dict = {}
            if static_mapping:
                static_mapping_dict = static_mapping_handler(data_dict, static_mapping)

            # 处理corpinfo地区信息
            area_dict = {}
            if api_name == 'corpinfo':
                area_dict = area_info_handler(data_dict)

            # 处理api与insert表映射
            api_table_dict = {}
            if api_table_mapping:
                if isinstance(api_table_mapping, dict):
                    # 适配api字段不全
                    for col in api_table_mapping.values():
                        data_dict[col] = None if col not in data_dict.keys() else data_dict[col]
                    api_values = [data_dict[col] for col in api_table_mapping.values() if col in data_dict.keys()]
                    api_table_dict = {k: v for k, v in zip(api_table_mapping.keys(), api_values)}
                elif isinstance(api_table_mapping, list):
                    for mapping in api_table_mapping:
                        api_values = [data_dict[col] for col in mapping.values() if col in data_dict.keys()]
                        api_table_dict = {k: v for k, v in zip(mapping.keys(), api_values)}
                else:
                    LOG.info('Error Configure Type!')

            res_dict = {**dynamic_mapping_dict, **static_mapping_dict, **area_dict, **api_table_dict, **default_mapping}

            # 特殊字段处理
            res_dict = special_columns_handler(api_name, data_dict, res_dict, url, domain, entity, download_res)
            exclude_li = [("scrmuserinfo", "TopicOfInterest"), ("hospital_equipment", "bizField")]
            for k, v in res_dict.items():
                if (entity, k) not in exclude_li:
                    res_dict[k] = str(v).replace('[', '').replace(']', '').replace("'", "").replace('"', '').replace(
                        '\\xa0', '')
                else:
                    if isinstance(v, list):
                        res_dict[k] = str([str(i) for i in v]).replace("'", '"')
            res_list.append(res_dict)

    return res_list


def _logger():
    """
    generate new logging object to log events
    :return: logging object
    """
    logger_obj = logging.getLogger()
    for handler in logger_obj.handlers:
        logger_obj.removeHandler(handler)
    logger_obj.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s: %(levelname)s : %(message)s')
    ch.setFormatter(formatter)
    logger_obj.addHandler(ch)
    return logger_obj
def _client(
        service_nm,
        max_attempts=3,
        timeout=5,
        region="cn-north-1",
        mode="adaptive",
        config_kwargs: Dict = None
):
    config_kwargs = config_kwargs or dict()
    adaptive_retries = Config(
        retries={
            "max_attempts": max_attempts,
            "mode": mode
        },
        connect_timeout=timeout,
        **config_kwargs
    )
    return boto3.client(service_nm, region_name=region, config=adaptive_retries)

LOCAL_ZONE = timezone("Asia/Shanghai")
weFeng = {
    'corpinfo': {'dynamic_mapping': {'original_col': ['customField', 'fieldCode'],
                                     'original_values': ['CustomField_25058', 'CustomField_25059', 'CustomField_25060',
                                                         'CustomField_25061', 'CustomField_24950', 'CustomField_24951',
                                                         'CustomField_24961', 'CustomField_24952', 'CustomField_24958',
                                                         'CustomField_24949', 'CustomField_24953', 'CustomField_24957',
                                                         'CustomField_24954', 'CustomField_24955', 'CustomField_24956',
                                                         'CustomField_27023', 'CustomField_27024', 'CustomField_27025',
                                                         'CustomField_27026'],
                                     # new_add_col：{新增字段1: 新增字段2}
                                     'new_add_col': {'CTMonthlyEnhanceExaminationTotal': '',
                                                     'CTEnhancementRate': '',
                                                     'MRMonthlyEnhanceExaminationTotal': '',
                                                     'MRIEnhancementRate': '',
                                                     'HospitalRank': 'HospitalRankStr',
                                                     'ChestPainCenterCertificationType': 'ChestPainCenterCertificationTypeStr',
                                                     'CheckPart': 'CheckPartStr',
                                                     'AffiliatedMedicalAssociation': '',
                                                     'DepartmentDevelopmentDirection': 'departmentDevelopmentDirectionStr',
                                                     'BusinessCode': '',
                                                     'Decil': '',
                                                     'IsCPA': '',
                                                     'BusinessRegion': '',
                                                     'BusinessManager': '',
                                                     'Represent': '',
                                                     'UVSales': '',
                                                     'MVSales': '',
                                                     'GVSales': '',
                                                     'PVSales': ''}},
                 'static_mapping': [{'mapping_method': 'comparison',  # 区间对比映射
                                     'original_col': {'corpSize': [Interval(0, 0), Interval(1, 4), Interval(5, 9),
                                                                   Interval(10, 19), Interval(20, 99),
                                                                   Interval(100, 499),
                                                                   Interval(500, 999), Interval(1000, 4999),
                                                                   Interval(5000, 9999), Interval(10000, 10000)]},
                                     'new_col': {'CorpSizeStr': ['null', '1', '2', '3', '4', '5', '6', '7', '8', '9']}},
                                    {'mapping_method': 'one_to_one',  # 一对一映射
                                     'original_col': {'industryTypeId': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                                                                         11, 12, 13, 14, 15, 16, 17, 18]},
                                     'new_col': {'industryTypestr': ['null', 'null', '金融', '医疗', '教育', '交通运输',
                                                                     '企业服务', '软件工具', '智能硬件', '消费零售',
                                                                     '体育',
                                                                     '旅游', '社交', '农业', '电子商务', '生活服务',
                                                                     '物流仓储', '房地产', '文娱传媒']}}
                                    ],
                 'api_table_mapping': {  # 结果表字段: api字段
                     'Id': 'id',
                     'CorpName': 'corpName',
                     'CorpNickname': 'corpNickname',
                     'CorpTel': 'corpTel',
                     'CorpAddress': 'corpAddress',
                     'CorpWebsite': 'corpWebsite',
                     'CorpLevel': 'corpLevel',
                     'CreateUserName': 'createUserName',
                     'UpdateUserName': 'updateUserName'
                 },
                 'default_mapping': {
                     'Last_updt_time': datetime.datetime.now(tz=LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S")}},
    'business_opportunity': {'dynamic_mapping': {},
                                'static_mapping': [],
                                'api_table_mapping': {
                                    'name':'name',
                                    'businesscategoryname':'businessCategoryName',
                                    'productname':'productName',
                                    'saleprocessname':'saleProcessName',
                                    'externalusername':'externalUserName',
                                    'externaluserid':'externalUserId',
                                    'unionid':'unionId',
                                    'corpid':'corpId',
                                    'corpname':'corpName',
                                    'corphiscode':'corpHisCode',
                                    'createtime':'createTime',
                                    'wxworkuserid':'wxworkUserId',
                                    'masterusername':'masterUserName',
                                    'flowuserid':'flowUserId',
                                    'flowusername':'flowUserName',
                                    'presigndate':'preSignDate',
                                    'presaleamount':'preSaleAmount',
                                    'customfield':'customField'
                                },
                                'default_mapping': {
                'feedType': '2', 'allRecordUrlList_recordType': None
                                }
                                },
# 设备信息
'hospital_equipment': {'dynamic_mapping': {},
                'static_mapping': [],
                'api_table_mapping': {
                    'id': 'id',
                    'createTime': 'createTime',
                    'updateTime': 'updateTime',
                    'wxworkUserId': 'wxworkUserId',
                    'wxworkUserName': 'wxworkUserName',
                    'bizType': 'bizType',
                    'externalUserId': 'externalUserId',
                    'externalUserName': 'externalUserName',
                    'externalRealName':'externalRealName',
                    'externalUserRemark': 'externalUserRemark',
                    'chatId': 'chatId',
                    'chatName': 'chatName',
                    'corpId': 'corpId',
                    'corpName': 'corpName',
                    'corpHisCode': 'corpHisCode',
                    'bizRecordTempId': 'bizRecordTempId',
                    'bizRecordTempName': 'bizRecordTempName',
                    'relationType': 'relationType',
                    'wecomSessionId': 'wecomSessionId',
                    'ccpsRecord': 'ccpsRecord',
                    'bizField': 'bizField'
                 },
                 'default_mapping': {
                     'FeedType':'2',
                     'LastUpdtTime': datetime.datetime.now(tz=LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S")}
                   },
    'meetinginfo': {'dynamic_mapping': {'original_col': ['bizField', 'fieldCode'],
                                        'original_values': ['CustomField_475', 'CustomField_471',
                                                            'CustomField_472', 'CustomField_473', 'CustomField_474'],
                                        'new_add_col': {'Remarks': '',
                                                        'Complete_time': '',
                                                        'MeetingCode': '',
                                                        'Metting_title': '',
                                                        'ParticipateTime': ''}
                                        },
                    'static_mapping': [],
                    'api_table_mapping': {'ScrmId': 'id',
                                          'Ucode': 'externalUserId',
                                          'Name': 'externalUserName',
                                          'Operator': 'wxworkUserName',
                                          'Cwid': 'wxworkUserId',
                                          'Created': 'createTime'},
                    'default_mapping': {'last_follow_time': None,
                                        'is_recruit': None,
                                        'follow_up': None,
                                        'follow_type': None,
                                        'send_email': None,
                                        'follow_state': None,
                                        'Communication_stage': None,
                                        'merit': None,
                                        'mobile': None,
                                        'Manner': None,
                                        'Product': None,
                                        'Stage_detail': None,
                                        'FeedType': '2',
                                        'Duration': None,
                                        'Last_updt_time': datetime.datetime.now(tz=LOCAL_ZONE).strftime(
                                            "%Y-%m-%d %H:%M:%S")
                                        }
                    },
    'scrmwechatbizrecord': {'dynamic_mapping': {'original_col': ['bizField', 'fieldCode'],
                                                'original_values': ['CustomField_465', 'CustomField_466',
                                                                    'CustomField_469', 'CustomField_470',
                                                                    'CustomField_495'],
                                                'new_add_col': {'Remarks': '',
                                                                'VisitType': 'VisitTypeStr',
                                                                'Manner': 'MannerStr',
                                                                'Product': 'ProductStr',
                                                                'CommunicationStage': 'CommunicationStageStr'
                                                                }
                                                },
                            'static_mapping': [],
                            'api_table_mapping': {
                                'Id': 'id',
                                'CreateTime': 'createTime',
                                'UpdateTime': 'updateTime',
                                'WxworkUserId': 'wxworkUserId',
                                'Cwid': 'wxworkUserId',
                                'WxworkUserName': 'wxworkUserName',
                                'BizType': 'bizType',
                                'ExternalUserId': 'externalUserId',
                                'ExternalUserName': 'externalUserName',
                                'ExternalUserRemark': 'externalUserRemark',
                                'ChatId': 'chatId',
                                'ChatName': 'chatName',
                                'CorpId': 'corpId',
                                'BizRecordTempId': 'bizRecordTempId',
                                'BizRecordTempName': 'bizRecordTempName',
                                'RelationType': 'relationType',
                                'WecomSessionId': 'wecomSessionId',
                            },
                            'default_mapping': {'StageDetail': None,
                                                'StageDetailStr': None}
                            },
    'mobileinfo': {'dynamic_mapping': {},
                   'static_mapping': [],
                   'api_table_mapping': {'Id': 'id',
                                         'Ucode': 'externalUserId',
                                         'Name': 'externalUserName',
                                         'Operator': 'wxworkUserName',
                                         'Represent': 'wxworkUserName',
                                         'Cwid': 'wxworkUserId',
                                         'Created': 'createTime'
                                         },
                   'default_mapping': {'CallType': '呼出',
                                       'last_follow_time': None,
                                       'is_recruit': None,
                                       'follow_up': None,
                                       'follow_type': None,
                                       'send_email': None,
                                       'follow_state': None,
                                       'complete_time': None,
                                       'merit': None,
                                       'qcellCore': None,
                                       'Communication_stage': None,
                                       'Product': None,
                                       'Remarks': None,
                                       'Manner': None,
                                       'Stage_detail': None,
                                       'Recording': None,
                                       'Mobile': None,
                                       'IsGethrough': None,
                                       'CallStartTime': None,
                                       'CallRingTime': None,
                                       'CallAnswerTime': None,
                                       'CallByeTime': None,
                                       'Duration': None,
                                       'FeedType': '2',
                                       'last_updt_Time': datetime.datetime.now(tz=LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S")
                                       }
                   },
    'smsinfo': {'dynamic_mapping': {},
                'static_mapping': [],
                'api_table_mapping': {
                    'ScrmId': 'id',
                    'Ucode': 'externalUserId',
                    'Created': 'createTime',
                    'MsgContent': 'textContent',
                    'Operator': 'creator',
                    'Cwid': 'createWxworkUserId'
                },
                'default_mapping': {
                    'remarks': None,
                    'last_follow_time': None,
                    'communication_stage': None,
                    'stage_detail': None,
                    'is_recruit': None,
                    'follow_up': None,
                    'follow_type': None,
                    'send_email': None,
                    'follow_state': None,
                    'complete_time': None,
                    'manner': None,
                    'merit': None,
                    'product': None,
                    'name': None,
                    'mobile': None,
                    'Last_updt_time': datetime.datetime.now(tz=LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S"),
                    'FeedType': '2'
                }
                },
    'jdksubmit': {'dynamic_mapping': {},
                  'static_mapping': [],
                  'api_table_mapping': {
                      'SubmitId': 'submitId',
                      'CourseId': 'courseId',
                      'CourseTitle': 'courseTitle',
                      'Avatar': 'avatar',
                      'CalendarId': 'calendarId',
                      'CalendarTitle': 'calendarTitle',
                      'UserId': 'userId',
                      'UserName': 'userName',
                      'UserMobile': 'userMobile',
                      'Unionid': 'unionid',
                      'ExternalUserId': 'externalUserId',
                      'Duration': 'duration',
                      'StudyDuration': 'studyDuration',
                      'AnswerScore': 'answerScore',
                      'QuestionCount': 'questionCount',
                      'ChoiceCount': 'choiceCount',
                      'ChoiceCorrectCount': 'choiceCorrectCount',
                      'CreatedAt': 'createdAt',
                      'UpdatedAt': 'updatedAt'

                  },
                  'default_mapping': {}
                  },
    'scrmassign': {'dynamic_mapping': {},
                   'static_mapping': [],
                   'api_table_mapping': {
                       'ExternalUserId': 'externalUserId',
                       'WxworkUserId': 'wxworkUserId',
                       'WxworkUserName': 'wxworkUserName',
                       'BeginTime': 'beginTime',
                   },
                   'default_mapping': {
                       'LastUpdtTime': datetime.datetime.now(tz=LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S"), }
                   },
    'materialinfo': {'dynamic_mapping': {},  # 两个url数据落到一张表
                     'static_mapping': [],
                     'api_table_mapping': {
                         'Id': 'id',
                         'GroupId': 'groupId',
                         'Name': 'name',
                         'Uri': 'uri',
                         'Type': 'type',
                         'FileType': 'fileType',
                         'Size': 'size',
                         'Description': 'description',
                         'CoverImage': 'coverImage',
                         'MiniappId': 'miniappId',
                         'ReadCnt': 'readCnt',
                         'ReadNum': 'readNum',
                         'UseCnt': 'useCnt',
                         'SignUpCnt': 'signUpCnt',
                         'Title': 'title',
                         'SignUpNum': 'signUpNum',
                         'Status': 'status'
                     },
                     'default_mapping': {
                         'cwid': None,
                         'lastUpdtTime': datetime.datetime.now(tz=LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S")
                     }
                     },
    'browsinfo': {'dynamic_mapping': {},  # 浏览接口
                  'static_mapping': [],
                  'api_table_mapping': {'Id': 'id',
                                        'ExternalUserid': 'externalUserid',
                                        'NewsType': 'newsType',
                                        'NewsTime': 'newsTime',
                                        'Content': 'content'
                                        },
                  'default_mapping': {
                      'lastUpdtTime': datetime.datetime.now(tz=LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S"),
                      'cwid': None,
                      'desc': None,
                      'customerName': None,
                      'count': None,
                      'materialTitle': None,
                      'materialType': None,
                      'duration': None,
                      'bottomFlag': None,
                      'submitFlag': None
                  }
                  },
    'scrmuserinfo': {'dynamic_mapping': {'original_col': ['customFields', 'fieldCode'],
                                         'original_values': ['CustomField_24943', 'CustomField_24962',
                                                             'CustomField_24939', 'CustomField_24941',
                                                             'CustomField_24940', 'CustomField_24942',
                                                             'CustomField_24944', 'CustomField_24945',
                                                             'CustomField_24946'],
                                         'new_add_col': {'TopicOfInterest': 'topicOfInterestStr',
                                                         'Sign': 'SignStr',
                                                         'Executive': 'ExecutiveStr',
                                                         'MedicalCareType': 'MedicalCareTypeStr',
                                                         'Departments': 'DepartmentStr',
                                                         'DoctorCode': '',
                                                         'SystemOfInterestTemp': 'SystemOfInterest',
                                                         'ProductOfInterestTemp': 'ProductOfInterest',
                                                         'NeedOfImprovementTemp': 'NeedOfImprovement'
                                                         }},  # 用户接口
                     'static_mapping': [],
                     'api_table_mapping': {  # insert_table: api
                         'ExternalUserId': 'externalUserId',
                         'RealName': 'realName',
                         'Mobile': 'mobile',
                         'CreateTime': 'addTimeWechat',
                         'Email': 'email',
                         'MajorFollowUser': 'majorFollowUser',
                         'MajorFollowUserName': 'majorFollowUserName',
                         'Name': 'name',
                         'WxUserFlag': 'wxUserFlag',
                         'Type': 'type',
                         'CorpFullName': 'corpFullName',
                         'CorpId': 'corpId',
                         'gender': 'gender',
                         'leadstatus': 'leadStatus',
                         'unionid': 'unionid'
                     },
                     'default_mapping': {'typeStr': None,
                                         'remarkName': None,
                                         'position': None,
                                         'isPotential': 'N',
                                         'isImportant': 'N'}
                     },
    'udeskmobileinfo': {'dynamic_mapping': {},  # 外呼接口
                        'static_mapping': [],
                        'api_table_mapping': {
                            'callid': 'callId',
                            'id': 'id',
                            'category': 'category',
                            'customernumber': 'customerNumber',
                            'beginat': 'beginAt',
                            'endat': 'endAt',
                            'displaynumber': 'displayNumber',
                            'agentname': 'agentName',
                            'customerringingtime': 'customerRingingTime',
                            'outlinenumber': 'outLineNumber',
                            'talktime': 'talkTime',
                            'followupcall': 'followUpCall',
                            'queuename': 'queueName',
                            'queueresult': 'queueResult',
                            'ringresult': 'ringResult',
                            'seqring': 'seqRing',
                            'outcallringtime': 'outCallRingTime',
                            'outcalldefeatcause': 'outCallDefeatCause',
                            'hangupby': 'hangupBy',
                            'callresult': 'callResult',
                            'messageurl': 'messageUrl',
                            'validflag': 'validFlag',
                            'surverylist': 'surveryList'
                        },
                        'default_mapping': {'feedType': '3', 'allRecordUrlList_recordType': None}
                        },
    'wechatcustomer': {'dynamic_mapping': {},  # 外呼接口
                       'static_mapping': [],
                       'api_table_mapping': {
                           'userid': 'externalUserId',
                           'typenum': 'type'
                       },
                       'default_mapping': {
                           'last_updt_time': datetime.datetime.now(tz=LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S")},
                           'talkrecord': None,
                           'cwid': None,
                           'airepcwid': None,
                           'username': None,
                           'mobile': None,
                           'businessname': None,
                           'createtime': None
                       }
}
LOG = _logger()
s3_client = _client("s3")
LOCAL_ZONE = timezone("Asia/Shanghai")
LOCAL_DATE = datetime.now(tz=LOCAL_ZONE).strftime("%Y%m%d%H%M%S")
YESTERDAY = datetime.strftime(datetime.now(tz=LOCAL_ZONE) - timedelta(days=1), '%Y%m%d%H%M%S')
LAST_YEAR_DATE = datetime.now(tz=LOCAL_ZONE) - timedelta(days=366)
year, month, day = tuple(datetime.now(tz=LOCAL_ZONE).strftime("%Y-%m-%d").split("-"))
request_params= (['https://ccps.s4.udesk.cn/api/v1/petitions/search?AppId=ae5742d5-7845-4cbe-6946-a7ed80bf8558&Timestamp=1701080412519&Token=bbd32f7e3eb095f4f32359cb501f8548020f0c3f'], {'Content-Type': 'application/json'}, 'json', {'pageSize': '1000', 'startTime': '2023-11-24 00:00:00', 'endTime': '2023-11-30 00:00:00', 'pageNum': '1'}, None, {'auth_func': 'get_token_by_sha1', 'base_url': 'https://ccps.s4.udesk.cn/api/v1', 'method': 'POST', 'history_load': {'range_type': 'datetime', 'datetime_format': '%Y-%m-%d %H:%M:%S', 'start_key': 'startTime', 'end_key': 'endTime', 'start_value': '2022-02-23 00:00:00', 'end_value': '2023-11-30 00:00:00', 'step': 518400}, 'download_params': {'type': 'https', 'download_key': 'talkRecord'}, 'params': {'AppId': 'ae5742d5-7845-4cbe-6946-a7ed80bf8558', 'Timestamp': '1701080412519', 'Token': 'bbd32f7e3eb095f4f32359cb501f8548020f0c3f'}, 'headers': {'Content-Type': 'application/json'}, 'body': {'type': 'json', 'prefix': '', 'post_offset_key': '', 'content': {'pageNum': 1, 'pageSize': 200, 'startTime': '', 'endTime': ''}}, 'response': {'next_page_key': ['pageNum'], 'data_content_key': 'data', 'data_count_key': 'paging.total'}, 'page_index_key': 'pageNum'}, 'https://ccps.s4.udesk.cn/api/v1', None)
domain= 'prestage_airepbi'
entity = 'mobileinfo'
landing_bucket = f"ph-cdp-landing-prod-cn-north-1"
load_id = '20231128200027'
post_url = 'https://bayer.wefeng360.com/api/v1/corp/list?sign=4aaa573d0769852ebd60192519603f86fd8cff64a19fec94cf3154b32b451a66&timestamp=1701224553217'
api_landing_s3 = f"s3://{landing_bucket}/{domain}/{load_id}/{entity}/{entity}"
entity_config ={
 "domain": "prestage_airepbi",
 "entity": "corpinfo",
 "api_url": "/corp/list",
 "is_soft_fail": "true",
 "landing_file_format": "json",
 "load_mode": "full",
 "ori_cols_sequence": "id,corpName,corpNickname,corpTel,corpAddress,corpWebsite,industryTypeId,industryTypeStr,corpSize,corpSizeStr,corpCity,corpProvince,corpCityStr,corpLevel,createUserName,updateUserName,Feed,Last_updt_time,CTMonthlyEnhanceExaminationTotal,CTEnhancementRate,MRMonthlyEnhanceExaminationTotal,MRIEnhancementRate,hospitalRank,hospitalRankStr,chestPainCenterCertificationType,chestPainCenterCertificationTypeStr,checkPart,checkPartStr,affiliatedMedicalAssociation,departmentDevelopmentDirection,departmentDevelopmentDirectionStr,businessCode,Decil,IsCPA,businessRegion,businessManager,represent,UVSales,MVSales,GVSales,PVSales",
 "post_contents": {
  "pageNum": "1",
  "pageSize": "1000"
 },
 "redshift_enriched_post_job": "TRUNCATE TABLE enriched_prestage_airepbi.corpinfo",
 "source_system": "api_wefeng",
 "standard_columns": "id,corpname,corpnickname,corptel,corpaddress,corpwebsite,industrytypeid,industrytypestr,corpsize,corpsizestr,corpcity,corpprovince,corpcitystr,corplevel,createusername,updateusername,feed,last_updt_time,ctmonthlyenhanceexaminationtotal,ctenhancementrate,mrmonthlyenhanceexaminationtotal,mrienhancementrate,hospitalrank,hospitalrankstr,chestpaincentercertificationtype,chestpaincentercertificationtypestr,checkpart,checkpartstr,affiliatedmedicalassociation,departmentdevelopmentdirection,departmentdevelopmentdirectionstr,businesscode,decil,iscpa,businessregion,businessmanager,represent,uvsales,mvsales,gvsales,pvsales",
 "state_machine_name": "ph-cdp-sm-workflow-cn-airepbi_etl_job_new",
 "time_delta": "0"
}
print(request_params[2])
print(request_params[5])

post_contents = request_params[3]
load_resp = api_load_to_s3(domain=domain, entity=entity,
                                       s3_path=api_landing_s3,
                                       url=post_url,
                                       headers=request_params[1],
                                       post_body_type=request_params[2],
                                       post_body=post_contents,
                                       proxies=request_params[4],
                                       entity_config=entity_config,
                                       api_conf=request_params[5])
print(load_resp)