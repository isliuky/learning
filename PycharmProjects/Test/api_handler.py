import ast
import hashlib
import hmac
import jsontest
import time
import random
import string
import warnings
from datetime import datetime, timedelta

import requests
import pandas as pd
from pytz import timezone

warnings.simplefilter(action='ignore')
from bayer_cdp_common_utils.client_generator import _client, _logger
from bayer_cdp_common_utils.s3_handler import s3_parser_to_bucket_prefix
from bayer_cdp_common_utils.api_data_source_mapping import weFeng, areaInfo, permissionRole
from bayer_cdp_common_utils.secret_manager_handler import get_secret
from bayer_cdp_common_utils.conf import ConfigGlobal
from bayer_cdp_common_utils.dsl import render
from bayer_cdp_common_utils.dynamo_handler import get_entity_config

LOG = _logger()
s3_client = _client("s3")
LOCAL_ZONE = timezone("Asia/Shanghai")
LOCAL_DATE = datetime.now(tz=LOCAL_ZONE).strftime("%Y%m%d%H%M%S")
YESTERDAY = datetime.strftime(datetime.now(tz=LOCAL_ZONE) - timedelta(days=1), '%Y%m%d%H%M%S')


def get_api_token(url, proxy, retry=3, interval=3):
    while retry > 0:
        try:
            response = requests.get(url, proxies=proxy).json()
            assert response.get("result") == 1, f"retrieve token failed, retry time {retry}"
            return response['data']['token']
        except Exception:
            time.sleep(interval)
            retry -= 1
    raise Exception(f"Get API token failed of url: {url}")


def get_api_data_count(url, headers, post_body_type, data_count_key, post_body, proxies=None, retry=10, interval=5):
    while retry > 0:
        try:
            if not post_body:
                resp = requests.get(url, headers=headers, proxies=proxies)
            else:
                resp = requests.post(url, headers=headers, data=post_body, proxies=proxies) \
                    if post_body_type.lower() != "json" else \
                    requests.post(url, headers=headers, json=post_body, proxies=proxies)
            data_count = resp.json().get(data_count_key)
            return data_count
        except Exception:
            retry -= 1
            time.sleep(interval)
    raise Exception(f"Post failed of url: {url} after {retry} retries")


def get_sign_by_encrypt_seq_params(api_source_params, conn_info, cw_id):
    nonce = ''.join(random.choices(string.hexdigits, k=16))
    timestamp = str(round(time.time() * 1000))

    preset_params = {'appId': '', 'appSignKey': '', 'current': '', 'cwid': '',
                     'nonce': nonce, 'size': '', 'timestamp': timestamp}
    for param_key, param_value in api_source_params.items():
        if not param_value and param_key in conn_info.keys():
            api_source_params[param_key] = conn_info.get(param_key)
        if not param_value and param_key not in conn_info.keys() and param_key in preset_params.keys():
            api_source_params[param_key] = preset_params[param_key]
    api_source_params.update({'cwid': cw_id})
    api_source_params = dict(sorted(api_source_params.items(), key=lambda d: d[0]))
    sign_params_list = [f"{k}={v}" for k, v in api_source_params.items()]
    sign = hashlib.sha256("&".join(sign_params_list).encode('utf-8')).hexdigest()
    sign_params_list = sign_params_list+["sign=" + sign]

    return sign_params_list


def area_info_handler(data_dict):
    # 通过corpCity获取省：corpProvince，市：corpCityStr，及特殊处理后的corpCity
    area_dict = {}.fromkeys(('corpCity', 'corpProvince', 'corpCityStr'))
    area_info = areaInfo
    if 'corpCity' in data_dict.keys():
        # for corp_city in data_dict['corpCity']:
        corp_city = data_dict['corpCity']
        if corp_city:
            city_str = None
            province_str = None
            for area in area_info:
                if isinstance(corp_city, list) and len(corp_city) > 0 and area['value'] == corp_city[0]:
                    province_str = area['label']
                elif isinstance(corp_city, str) and area['value'] == corp_city:
                    province_str = area['label']
                if province_str:
                    if isinstance(corp_city, list) and len(corp_city) > 1 and 'children' in area.keys():
                        cities = [child['label'] if child['value'] == corp_city[1] else [] for child in
                                  area['children']]
                        city = [city for city in cities if city]
                        city_str = city[0] if len(city) > 0 else None
                    break
            corp_city_str = str(corp_city).replace('[', '').replace(']', '').replace('"', '')
            area_dict['corpCity'] = [corp_city_str]
            area_dict['corpProvince'] = [province_str]
            if city_str and province_str not in ('北京市', '上海市', '天津市', '重庆市'):
                area_dict['corpCityStr'] = [city_str]
            else:
                area_dict['corpCityStr'] = [province_str]
    return area_dict


def is_instance(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def dynamic_mapping_handler(data_dict, original_col, original_values, new_add_col_dict):
    fields = data_dict[original_col[0]]
    # 处理新增列1：默认获取content
    fields_df = pd.DataFrame(fields)
    original_values_df = pd.DataFrame(list(zip(original_values, new_add_col_dict.keys(), new_add_col_dict.values())),
                                      columns=[original_col[1], 'new_col_1', 'new_col_2'])
    new_col_values = {}

    if fields:
        # if data_dict['externalUserId'] == 'wmX8fADwAACuBBgGiXJrkEB4qxLa2unQ':
        content_df = pd.merge(fields_df, original_values_df, on=original_col[1], how='right')
        content_df['content'] = content_df['content'].fillna('')
        for index, value in content_df['content'].items():
            if value and isinstance(value, list):
                content_df['content'].iloc[index] = [int(x) if is_instance(x) else x for x in value]
            elif value and is_instance(value):
                content_df['content'].iloc[index] = int(value)

        new_col_1_values = {k: v for k, v in zip(content_df['new_col_1'], content_df['content'])}

        # new_col_1_values = {k: [] for k in }
        new_col_2_values = {}
        content_df.fillna('')
        for index, rows in content_df.iterrows():
            if rows['new_col_2'] and rows['new_col_2'] != '' and not isinstance(rows['new_col_2'], float):
                if rows['content'] and rows['optionList'] and isinstance(rows['content'], list):
                    option_dict = {option['key']: option['value'] for option in rows['optionList']}
                    new_col_2_values.update({rows['new_col_2']: ["".join(option_dict[content].split()) for content in rows['content'] if
                                             content in option_dict.keys()]})  # 替换\xa0字符
                elif rows['content'] and rows['optionList'] and isinstance(rows['content'], str):
                    new_col_2_values.update({rows['new_col_2']: option['value'] for option in rows['optionList']
                                             if option['key'] !='' and rows['content'] !=''
                                             and option['key'] == rows['content']})
                elif rows['content'] and rows['optionList'] and isinstance(rows['content'], int):
                    new_col_2_values.update({rows['new_col_2']: option['value'] for option in rows['optionList']
                                             if option['key'] != '' and rows['content'] != ''
                                             and int(option['key']) == int(rows['content'])})
                else:
                    new_col_2_values.update({rows['new_col_2']: None})
        new_col_values = {**new_col_1_values, **new_col_2_values}
    return new_col_values


def static_mapping_handler(data_dict, static_mapping):
    static_dict = {}

    comparison_dict = {}
    one_to_one_dict = {}
    original_dict = {}
    static_ist = []
    for mappings in static_mapping:
        static_original_col, static_new_col = list(mappings['original_col'].keys())[0], \
                                                   list(mappings['new_col'].keys())[0]
        if static_original_col in data_dict.keys():
            original_values = mappings['original_col'][static_original_col]
            new_values = mappings['new_col'][static_new_col]
            mapping_method = mappings['mapping_method']
            if mapping_method == 'comparison':  # 区间对比映射
                # original_value = data[static_original_col]
                for interval_list in original_values:
                    if data_dict[static_original_col] is not None and data_dict[static_original_col] in interval_list:
                        index = original_values.index(interval_list)
                        new_value = new_values[index]
                    else:
                        new_value = 'null'
                    comparison_dict = {static_original_col: data_dict[static_original_col], static_new_col: new_value}
            if mapping_method == 'one_to_one':  # 一对一映射
                for original_value in original_values:
                    if data_dict[static_original_col] is not None and data_dict[static_original_col] == original_value:
                        index = original_values.index(original_value)
                        new_value = new_values[index]
                        one_to_one_dict = {static_original_col: data_dict[static_original_col], static_new_col: new_value}
                    else:
                        one_to_one_dict = {static_original_col: data_dict[static_original_col], static_new_col: 'null'}
            static_dict = {**comparison_dict, **one_to_one_dict}
    return static_dict


def fields_convert(field_name, field):
    res_value = ''
    if field_name and isinstance(field_name, list):
        res_value = [option['value'] for detail in field_name
                                           for option in field['optionList']
                                           if option['key'] and field_name
                                           and int(detail) == int(option['key'])]
    elif field_name:
        res_value = [option['value'] for option in field['optionList'] if option['key'] and field_name
                                           and int(option['key']) == int(field_name)]

    return res_value


def multi_layered_fields_convert(inner_dict, field_name):
    inner_field = inner_dict[field_name] if field_name in inner_dict.keys() else None
    return inner_field


def special_columns_handler(api_name,data_dict, res_dict, url):
    res_dict['Feed'] = url.split('?')[0]
    if api_name == 'corpinfo':
        if 'corpSize' in res_dict.keys() and not res_dict['corpSize']:
            res_dict['corpSize'] = 0
        if 'CorpSizeStr' in res_dict.keys():
            res_dict['CorpSizeStr'] = None if res_dict['CorpSizeStr'] == 'null' else res_dict['CorpSizeStr']
        if 'industryTypestr' in res_dict.keys():
            res_dict['industryTypestr'] = None if res_dict['industryTypestr'] == 'null' else res_dict['industryTypestr']

    if api_name == 'meetinginfo':
        if 'ParticipateTime' in res_dict.keys():
            participate_time = res_dict['ParticipateTime']
            if participate_time:
                res_dict['ParticipateTime'] = res_dict['ParticipateTime'].replace("：", ":").replace("(?i)AM", "").replace(
            "(?i)PM", "")
            else:
                res_dict['ParticipateTime'] = None
        if 'bizField' in data_dict.keys():
            biz_field = data_dict['bizField']
            if isinstance(biz_field, list):
                for field in biz_field:
                    field_code = field['fieldCode']
                    if 'content' in field.keys():
                        if field_code == 'CustomField_474' and field['content']:
                            duration = field['content'].split(':')
                            if duration:
                                if len(duration) == 3:
                                    res_dict['Duration'] = str(int(duration[0]) * 60 + int(duration[1]) + 1) \
                                                            if int(duration[2])>0 \
                                                            else str(int(duration[0]) * 60 + int(duration[1]) + 0)
                                elif len(duration) == 2:
                                    res_dict['Duration'] = str(int(duration[0]) + 1) \
                                                            if int(duration[1]) > 0 \
                                                            else str(int(duration[0]) + 0)
                                else:
                                    res_dict['Duration'] = None
    elif api_name == 'mobileinfo':
        if 'bizField' in data_dict.keys():
            biz_field = data_dict['bizField']
            if isinstance(biz_field, list):
                for field in biz_field:
                    field_code = field['fieldCode']
                    if 'content' in field.keys():
                        if field_code == 'CustomField_495':
                            res_dict['Communication_stage'] = fields_convert(field['content'], field)
                        elif field_code == 'CustomField_470':
                            res_dict['Product'] = fields_convert(field['content'], field)
                        elif field_code == 'CustomField_465':
                            res_dict['Remarks'] = field['content']
                        elif field_code == 'CustomField_469':
                            res_dict['Manner'] = fields_convert(field['content'], field)
                        elif field_code in ('CustomField_496', 'CustomField_497', 'CustomField_531', 'CustomField_533'):
                            stage_detail = fields_convert(field['content'], field)
                            if stage_detail:
                                res_dict['Stage_detail'] = stage_detail

        if 'ccpsRecord' in data_dict.keys():
            ccps_record = data_dict['ccpsRecord']
            if ccps_record:
                if 'wxworkUserId' in data_dict.keys() and ccps_record['recfile']:
                    res_dict['Recording'] = r"""\\\\BCNSHGS0295\\02_FlatDataSource\\02_Backup\\58_AI_Rep\\recording\\""" + \
                    str(ccps_record['startTime']).replace("-", "").replace(" ", "-").replace(":", "") + \
                    "-" + data_dict['wxworkUserId'] + "-" + ccps_record['calledNumber'] + "." + \
                    ccps_record['recfile'].split('?')[0].split('.')[-1]
                res_dict['Mobile'] = ccps_record['calledNumber'] if 'calledNumber' in ccps_record.keys() else None
                res_dict['IsGethrough'] = ccps_record['callResult'] if 'callResult' in ccps_record.keys() else None
                res_dict['CallStartTime'] = ccps_record['startTime'] if 'startTime' in ccps_record.keys() else None
                res_dict['CallRingTime'] = ccps_record['startTime'] if 'startTime' in ccps_record.keys() else None
                res_dict['CallAnswerTime'] = ccps_record['answeredTime'] if 'answeredTime' in ccps_record.keys() else None
                res_dict['CallByeTime'] = ccps_record['hangupTime'] if 'hangupTime' in ccps_record.keys() else None
                res_dict['Duration'] = int(int(ccps_record['callDuration']) / 1000) if 'callDuration' in ccps_record.keys() \
                                                                                  and ccps_record['callDuration'] else None
    elif api_name == 'scrmwechatbizrecord':
        res_dict['lastUpdtTime'] = datetime.now(tz=LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S")
        if 'bizField' in data_dict.keys():
            biz_field = data_dict['bizField']
            if isinstance(biz_field, list):
                for field in biz_field:
                    field_code = field['fieldCode']
                    if 'content' in field.keys() and 'CommunicationStage' in res_dict.keys():
                        if (field_code == 'CustomField_496' and res_dict['CommunicationStage'] == 1) or \
                           (field_code == 'CustomField_497' and res_dict['CommunicationStage'] == 2) or \
                           (field_code == 'CustomField_531' and res_dict['CommunicationStage'] == 3) or \
                           (field_code == 'CustomField_533' and res_dict['CommunicationStage'] == 4):
                            res_dict['StageDetail'] = field['content']
                        if res_dict['StageDetail'] and 'optionList' in field.keys():
                            if isinstance(res_dict['StageDetail'], list):
                                res_dict['StageDetailStr'] = [option['value'] for detail in res_dict['StageDetail']
                                                                              for option in field['optionList']
                                                                              if option['key'] and res_dict['StageDetail']
                                                                              and int(detail) == int(option['key'])]
                            else:
                                res_dict['StageDetailStr'] = [option['value'] for option in field['optionList']
                                                              if option['key'] and res_dict['StageDetail']
                                                              and int(option['key']) == int(res_dict['StageDetail'])]
    elif api_name == 'scrmuserinfo':
        if 'CorpId' in res_dict.keys() and not res_dict['CorpId']:
            res_dict['CorpId'] = 0
        if 'gender' in data_dict.keys():
            if data_dict['gender'] == 0:
                res_dict['genderStr'] = '未知'
            elif data_dict['gender'] == 1:
                res_dict['genderStr'] = '男性'
            elif data_dict['gender'] == 2:
                res_dict['genderStr'] = '女性'
            else:
                res_dict['genderStr'] = None
        if 'customFields' in data_dict.keys():
            custom_fields = data_dict['customFields']
            if isinstance(custom_fields, list):
                for field in custom_fields:
                    field_code = field['fieldCode']
                    if field_code == 'CustomField_27331' and 'content' in field.keys():
                        res_dict['isPotential'] = 'Y' if str(field['content']).__contains__('1') else 'N'
                        res_dict['isImportant'] = 'Y' if str(field['content']).__contains__('2') else 'N'
        if 'externalTags' in data_dict.keys() and data_dict['externalTags']:
            res_dict['externalTags'] = [res['tagName'] if 'tagName' in res.keys() else '' for res in data_dict['externalTags']]
        if 'flowUsers' in data_dict.keys() and data_dict['flowUsers']:
            add_time_list = [res['addTime'] if 'addTime' in res.keys() else '' for res in data_dict['flowUsers']]
            if len(add_time_list) > 0:
                max_add_time = max(add_time_list)
                res_dict['flowUsers'] = [res['userId'] for res in data_dict['flowUsers'] if 'userId' in res.keys()
                                                       and res['addTime']==max_add_time]
        for k in list(res_dict.keys()):
            if k.endswith('Temp'):
                del res_dict[k]
    elif api_name == 'browsinfo':
        if 'dataRadarResponse' in data_dict.keys():
            data_res = data_dict['dataRadarResponse']
            if data_res and 'material4DataRadarResponseList' in data_res.keys():
                res_dict['desc'] = data_dict['desc'] if 'desc' in data_dict.keys() else None
                res_list = data_res['material4DataRadarResponseList']
                for res in res_list:
                    res_dict['customerName'] = multi_layered_fields_convert(res, 'customerName')
                    res_dict['count'] = multi_layered_fields_convert(res, 'count')
                    res_dict['materialTitle'] = multi_layered_fields_convert(res, 'materialTitle')
                    res_dict['materialType'] = multi_layered_fields_convert(res, 'materialType')
                    res_dict['duration'] = multi_layered_fields_convert(res, 'duration')
                    res_dict['bottomFlag'] = multi_layered_fields_convert(res, 'bottomFlag')
                    res_dict['submitFlag'] = multi_layered_fields_convert(res, 'submitFlag')
    elif api_name == 'udeskmobileinfo':
        del res_dict['Feed']
        res_dict['surverylist'] = str(res_dict['surverylist']) if 'surverylist' in res_dict.keys() else None
        if 'agentInfo' in data_dict.keys():
            agent_info = data_dict['agentInfo']
            res_dict['agentinfo_id'] = multi_layered_fields_convert(agent_info, 'id') if agent_info else None
            res_dict['agentinfo_name'] = multi_layered_fields_convert(agent_info, 'name') if agent_info else None
            res_dict['agentinfo_employeeid'] = multi_layered_fields_convert(agent_info, 'employeeId') if agent_info else None
            res_dict['agentinfo_number'] = multi_layered_fields_convert(agent_info, 'number') if agent_info else None
        if 'phoneInfo' in data_dict.keys():
            phone_info = data_dict['phoneInfo']
            res_dict['phoneinfo_number'] = multi_layered_fields_convert(phone_info, 'Number') if phone_info else None
            res_dict['phoneinfo_province'] = multi_layered_fields_convert(phone_info, 'Province') if phone_info else None
            res_dict['phoneinfo_city'] = multi_layered_fields_convert(phone_info, 'City') if phone_info else None
            res_dict['phoneinfo_operator'] = multi_layered_fields_convert(phone_info, 'Operator') if phone_info else None
        if 'agentName' in data_dict.keys() and 'customerNumber' in data_dict.keys() and \
            'talkRecord' in data_dict.keys() and 'beginAt' in data_dict.keys():
            recording_file = r"\\\\BCNSHGS0295\\02_FlatDataSource\\02_Backup\\58_AI_Rep\\recording\\"
            begin_at = data_dict['beginAt']
            talk_record = data_dict['talkRecord']
            if begin_at and talk_record:
                begin_at = begin_at.replace("-", "").replace(" ", "-").replace(":", "")
                talk_record = talk_record.split('?')[0].split('.')[-1]
                res_dict['talkrecord'] = recording_file + begin_at + "-" + data_dict['agentName'] + \
                                         "-" + data_dict['customerNumber'] + "." + talk_record
        if 'agentName' in data_dict.keys():
            agent_name = data_dict['agentName']
            index = [v.index(agent_name) for k, v in permissionRole.items() if agent_name in v]
            if len(index) > 0:
                res_dict['cwid'] = permissionRole['CWID'][index[0]]
    elif api_name == 'wechatcustomer':
        res_dict['userid'] = str(res_dict['userid']) if 'userid' in res_dict.keys() else None
        res_dict['typenum'] = str(res_dict['typenum']) if 'typenum' in res_dict.keys() else None
        if 'followUser' in data_dict.keys():
            follow_user_list = data_dict['followUser']
            if follow_user_list:
                for follow_user in follow_user_list:
                    res_dict['airepcwid'] = multi_layered_fields_convert(follow_user, 'extId')
                    res_dict['username'] = multi_layered_fields_convert(follow_user, 'remark')
                    res_dict['mobile'] = multi_layered_fields_convert(follow_user, 'remarkMobiles')
                    res_dict['businessname'] = str(follow_user['remarkCorpName']) if 'remarkCorpName' in follow_user.keys() else None
                    res_dict['createtime'] = str(follow_user['createTime']) if 'createTime' in follow_user.keys() else None
    elif api_name == 'materialinfo':
        if 'folder' in url:
            res_dict['FeedType'] = '1'
        elif 'markting' in url:
            res_dict['FeedType'] = '2'
        else:
            res_dict['FeedType'] = None
    return res_dict


def wefeng_data_source_handler(api_name, url, data, bucket):
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
            res_dict = special_columns_handler(api_name, data_dict, res_dict, url)
            for k, v in res_dict.items():
                if k != 'TopicOfInterest':
                    res_dict[k] = str(v).replace('[', '').replace(']', '').replace("'", "").replace('"', '').replace('\\xa0', '')
                else:
                    if isinstance(v, list):
                        res_dict[k] = str([str(i) for i in v]).replace("'", '"')
            res_list.append(res_dict)

    return res_list


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


def get_auth_info(domain, entity, api_url):
    entity_config = get_entity_config(domain, entity)
    system_nm = entity_config['source_system'].split("_")[0] # api
    conn_info = get_secret(f"phcdp/{system_nm}/" + domain)[1]  # 连接secret manager
    api_conf = ConfigGlobal.api_source_conf  # 获取config
    domain_conf = api_conf.get(domain)
    api_source_conf = domain_conf.get(api_url)
    api_source_params = api_source_conf.get("params")
    headers = api_source_conf.get("headers")
    post_body_type = api_source_conf.get("body").get("type")
    base_url = domain_conf[api_url]['base_url'] if 'base_url' in domain_conf[api_url].keys() \
        else conn_info['base_url']

    load_mode = entity_config['load_mode']

    if conn_info.get('proxy_tcp') is not None:
        proxies = {conn_info['proxy_tcp']: f"{conn_info['proxy_name']}:{conn_info['proxy_password']}@"
                                           f"{conn_info['proxy_ip']}:{conn_info['proxy_port']}"}
    else:
        proxies = None

    data_url_list = []
    timestamp = str(round(time.time() * 1000))
    app_id = conn_info['appId'] if 'appId' in conn_info.keys() else ''
    secret = conn_info['secret'] if 'secret' in conn_info.keys() else ''

    auth_func = domain_conf[api_url]['auth_func'] if 'auth_func' in domain_conf[api_url].keys() else domain_conf[
        'auth_func']  # get_sign_by_sha256
    if auth_func == 'get_token_by_url':
        token_url = conn_info.get("get_token_url") if not conn_info.get("get_token_url").endswith(
            "/") else conn_info.get(
            "get_token_url")[:-1]
        get_token_conf = domain_conf.get(token_url).get("params")
        for k, v in get_token_conf.items():
            if not v:
                get_token_conf[k] = conn_info.get(k)
        token_params_list = [f"{k}={v}" for k, v in get_token_conf.items()]
        get_token_url = base_url + token_url + "?" + "&".join(token_params_list)
        token = get_api_token(get_token_url, proxies)
        api_source_params['token'] = token
        sign_params_list = [f"{k}={v}" for k, v in api_source_params.items()]
        data_url_list = [f"{base_url}{api_url}?" + "&".join(sign_params_list)]
    elif auth_func == 'get_sign_by_sha256':
        get_sign_conf = domain_conf.get(api_url)
        sign_params = get_sign_conf.get("params")
        for k, v in sign_params.items():
            if not v:
                sign_params[k] = conn_info.get(k)
            if k == 'timestamp':
                sign_params[k] = timestamp
        sign_str = '&'.join(sign_params.values())
        sign = hashlib.sha256(sign_str.encode('utf-8')).hexdigest()
        sign_params['sign'] = sign
        sign_params_list = [f"{k}={v}" for k, v in sign_params.items()]
        data_url_list = [f"{base_url}{api_url}?" + "&".join(sign_params_list)]

    elif auth_func == 'get_token_by_sha1':
        key = bytes(secret, 'UTF-8')
        message = bytes(app_id + timestamp, 'UTF-8')
        digester = hmac.new(key, message, hashlib.sha1)
        token = digester.hexdigest()
        preset_params = {'AppId': app_id, 'Timestamp': timestamp, 'Token': token}
        for param_key, param_value in api_source_params.items():
            if not param_value and param_key in conn_info.keys():
                api_source_params[param_key] = conn_info.get(param_key)
            if not param_value and param_key not in conn_info.keys() and \
                    (param_key in preset_params.keys() or param_key.lower() in preset_params.keys()):
                api_source_params[param_key] = preset_params[param_key]
        sign_params_list = [f"{k}={v}" for k, v in api_source_params.items()]
        data_url_list = [f"{base_url}{api_url}?" + "&".join(sign_params_list)]

    elif auth_func == 'get_sign_by_encrypt_seq_params':
        if entity == 'wechatcustomer':
            all_api_params = []
            for cw_id in permissionRole['CWID']:
                sign_params_list = get_sign_by_encrypt_seq_params(api_source_params, conn_info, cw_id)
                all_api_params.append(sign_params_list)
            data_url_list = [f"{base_url}{api_url}?" + "&".join(api_params) for api_params in all_api_params]

    else:
        raise Exception("The authorization method is unsupported, please check configuration!")

    # load_mode = entity_config['load_mode']
    request_method = api_source_conf.get('method')
    post_offset = ""
    if request_method == 'POST':
        if load_mode in ('customized', 'full'):
            post_contents = entity_config['post_contents']
            for k, v in post_contents.items():
                post_contents[k] = render(v)
            post_offset = post_contents.get(api_source_conf.get("body").get("post_offset_key"))
            body_type = api_source_conf.get("body").get('type')
            if body_type == 'text':
                post_contents_prefix = api_source_conf.get("body").get('prefix', '')
                post_contents = (post_contents_prefix + str(post_contents)).replace("'", '"')
        elif load_mode == "incremental":
            post_contents = ''
        else:
            raise Exception(f"Unsupported load mode found: {load_mode}, "
                            f"load_mode should be in one in ('customized', 'incremental' or 'full') ")
    else:
        post_contents = ''

    return data_url_list, headers, post_body_type, post_contents, proxies, api_source_conf, base_url, \
        post_offset


def convert_data_type(data_list):
    if data_list:
        data_df = pd.DataFrame(data_list)
        data_df = data_df.fillna('')
        for col in data_df.columns:
            data_df[col] = data_df[col].apply(lambda x: None if x in ('None', 'nan', 'NULL') else x)
        data_list = data_df.to_dict('records')
    return data_list


def api_load_to_s3(domain, entity, s3_path, url, api_url, headers, post_body_type, post_body, proxies, entity_config, api_conf: dict) -> (str, int):
    bucket, prefix, _ = s3_parser_to_bucket_prefix(s3_path)
    source_path = []
    result_list = []
    source_row_count = 0
    page_no = 0
    while True:
        if post_body_type == 'text':
            post_body_prefix = api_conf.get("body")["prefix"]
            post_body_dict = ast.literal_eval(post_body.split(post_body_prefix)[-1])
            if page_no == 0:
                post_body_dict[api_conf.get("page_index_key")] = int(post_body_dict[api_conf.get("page_index_key")])
            else:
                post_body_dict[api_conf.get("page_index_key")] = int(post_body_dict[api_conf.get("page_index_key")]) + 1
            post_body = post_body_prefix + str(post_body_dict)
        elif post_body_type == 'json' and 'pageNum' in post_body.keys():
            if page_no > 0:
                post_body['pageNum'] = str(int(post_body['pageNum']) + 1)
        if post_body:
            LOG.info(f'post_body: {post_body}')
        else:
            LOG.info(f'url: {url.split("?")[1]}')
        response = dynamic_post(url, headers, post_body_type, post_body, proxies).json()

        if response.get('code') == 400 and response.get('message') == "validate.api.timeout.error":
            request_params = get_auth_info(domain, entity, api_url)
            url = request_params[0]
            response = dynamic_post(url, headers, post_body_type, post_body, proxies).json()

        response_conf = api_conf.get('response')
        data_count_key = response_conf.get('data_count_key')
        pages_data_list = response.get(response_conf.get("data_content_key"))

        page_no += 1
        output_key = f'{prefix}' + datetime.now(tz=LOCAL_ZONE).strftime("%Y%m%d%H%M%S") + ".json"
        if pages_data_list:
            if data_count_key:
                keys = data_count_key.split(".")
                data_count = response.get(keys[0]).get(keys[1]) if len(keys) == 2 else response.get(keys[0])
                LOG.info(f'data_count: {data_count}')
            else:
                data_count = len(pages_data_list) + 1
                LOG.info(f'pages_data_count: {len(pages_data_list)}')

            if domain == 'prestage_airepbi':
                pages_data_list = wefeng_data_source_handler(entity, url, pages_data_list, bucket)
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
    if result_list.__len__() > 0:
        output_key = f'{prefix}' + datetime.now(tz=LOCAL_ZONE).strftime("%Y%m%d%H%M%S") + ".json"
        s3_client.put_object(
            Body=json.dumps(result_list),
            Bucket=bucket,
            Key=output_key
        )
        source_path.append(output_key)
    return ",".join(source_path), source_row_count