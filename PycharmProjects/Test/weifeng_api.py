import requests
from hashlib import sha256
import time
import hashlib
import datetime
import hmac
import re
from typing import List, Dict, Any
import pandas as pd
import sqlite3
api_source_conf = {
    "prestage_airepbi": {"auth_func": "get_sign_by_sha256",
                         "/corp/list": {
                             "method": "POST",
                             "params": {
                                 "sign": "",
                                 "timestamp": str(int(time.time()))
                             },
                             "headers": {"Content-Type": "application/json"},
                             "body": {
                                 "type": "json",
                                 "prefix": "",
                                 "post_offset_key": "",
                                 "content": {
                                     "pageNum": 1,
                                     "pageSize": 1000}},
                             "response": {
                                 "next_page_key": ["paging", "total"],
                                 "data_content_key": "data",
                                 "data_count_key": "paging.total"
                             },
                             "page_index_key": "pageNum"
                         },
                         "/externalContact/list": {  # 用户接口
                             "method": "POST",
                             "params": {
                                 "sign": "",
                                 "timestamp": str(int(time.time()))
                             },
                             "headers": {"Content-Type": "application/json"},
                             "body": {
                                 "type": "json",
                                 "prefix": "",
                                 "post_offset_key": "",
                                 "content": {"pageNum": 1,
                                             "pageSize": 1000
                                             }
                             },
                             "response": {
                                 "next_page_key": ["paging", "total"],
                                 "data_content_key": "data",
                                 "data_count_key": "paging.total"
                             },
                             "page_index_key": "pageNum"
                         },
                         "/petitions/search": {
                                 "auth_func": 'get_token_by_sha1',
                                 "base_url": 'https://ccps.s4.udesk.cn/api/v1',
                                 "method": "POST",
                                 "history_load": {
                                     "range_type": "datetime",
                                     "datetime_format": "%Y-%m-%d %H:%M:%S",
                                     "start_key": "startTime",
                                     "end_key": "endTime",
                                     "start_value": "2022-02-23 00:00:00",
                                     "end_value": (datetime.datetime.now() + datetime.timedelta(days=3)).strftime("%Y-%m-%d 00:00:00"),
                                     "step": 6 * 24 * 60 * 60
                                 },
                                 "download_params": {
                                     "type": "https",
                                     "download_key": "talkRecord"
                                 },
                                 "params": {
                                     "AppId": "",
                                     "Timestamp": '',
                                     "Token": '',
                                 },
                                 "headers": {"Content-Type": "application/json"},
                                 "body": {
                                     "type": "json",
                                     "prefix": "",
                                     "post_offset_key": "",
                                     "content": {"pageNum": 1,
                                                 "pageSize": 200,
                                                 "startTime": '',
                                                 "endTime": '',
                                                 }
                                 },
                                 "response": {
                                     "next_page_key": ["pageNum"],
                                     "data_content_key": "data",
                                     "data_count_key": "paging.total"
                                 },
                                 "page_index_key": "pageNum"
                             },
                         "/customer/get-list": {  # WeChatCustomer
                             "auth_func": 'get_sign_by_encrypt_seq_params',
                             "base_url": 'https://wecomapi.bayer.cn/open/clientapps',
                             "method": "GET",
                             "params": {
                                 "appId": "108",
                                 "appSignKey": "",
                                 "current": "1",
                                 "cwid": "",
                                 "nonce": "",
                                 "size": '1000',
                                 "timestamp": '',
                             },
                             # "headers": {"Content-Type": "application/json"},
                             "body": {},
                             "response": {
                                 "next_page_key": ["current"],
                                 "data_content_key": "records",
                                 "data_count_key": "total"
                             },
                             "page_index_key": "current",
                         },
                         "/niche/list": {
                             "method": "POST",
                             "params": {
                                 "sign": "",
                                 "timestamp": str(int(time.time()))
                             },
                             "headers": {"Content-Type": "application/json"},
                             "body": {
                                 "type": "json",
                                 "prefix": "",
                                 "post_offset_key": "",
                                 "content": {
                                     "pageNum": 1,
                                     "pageSize": 1000}},
                             "response": {
                                 "next_page_key": ["paging", "total"],
                                 "data_content_key": "data",
                                 "data_count_key": "paging.total"
                             },
                             "page_index_key": "pageNum"
                         },
                         "/externalContact/searchBizRecord": {
                             "method": "POST",
                             "params": {
                                 "sign": "",
                                 "timestamp": str(int(time.time()))
                             },
                             "headers": {"Content-Type": "application/json"},
                             "body": {
                                 "type": "json",
                                 "prefix": "",
                                 "post_offset_key": "",
                                 "content": {
                                     "pageNum": 1,
                                     "pageSize": 1000}},
                             "response": {
                                 "next_page_key": ["paging", "total"],
                                 "data_content_key": "data",
                                 "data_count_key": "paging.total"
                             },
                             "page_index_key": "pageNum"
                         },
                         },

}
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
_PAT = re.compile(r"{{[^{}]*}}")
def expression_parse(string) -> List[str]:
    exps = re.findall(_PAT, string)
    return exps
def run_query(query) -> Dict[str, Any]:
    with sqlite3.connect(":memory:") as connection:
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            row = cursor.fetchone()
        except sqlite3.OperationalError as e:
            raise ValueError(f"Execute query failed: {query}, error: {e}")
        if row:
            return dict(row)
        return dict()
def expression_query(exp):
    function = exp.replace("{", "").replace("}", "").strip()
    function = _alias.get(function.upper(), function)
    try:
        query = f"SELECT {function} AS result"
        ret = run_query(query)
        if ret:
            return ret["result"]
        raise ValueError(f"Invalid expression: {exp}, result is None")
    except Exception:
        return exp

def render(string: str):
    """A string containing expressions can be dynamically rendered based on SQLite functions.
    To ensure proper evaluation, expressions must be encapsulated within double curly braces '{{}}'.

    # >>> render("s3://landing/appddm_{{date('now', 'localtime')}}/{{strftime('%Y%m%d','now', 'localtime')}}.xlsx")
    # 's3://landing/appddm_2023-02-08/20230208.xlsx'

    :param string:
    """
    exps = expression_parse(string)
    results = {exp: expression_query(exp) for exp in exps}
    for k, v in results.items():
        string = string.replace(k, str(v))
    return string

def get_auth_info():
    timestamp = str(int(time.time()))
    domain_conf = api_source_conf.get('prestage_airepbi')
    # api_url = "/externalContact/searchBizRecord"
    api_url = "/corp/list"
    get_sign_conf = domain_conf.get(api_url)
    sign_params = get_sign_conf.get("params")
    api_source_params = api_source_conf.get("params")
    print(f"------------api_source_params:{api_source_params}-------------")
    # headers = api_source_conf.get("headers")
    # post_body_type = api_source_conf.get("body").get("type")
    base_url = domain_conf[api_url]['base_url'] if 'base_url' in domain_conf[api_url].keys() \
        else conn_info['base_url']

    # base_url ="https://bayer.wefeng360.com/api/v1"
    # base_url= 'https://ccps.s4.udesk.cn/api/v1'
    for k, v in sign_params.items():
        if not v:
            sign_params[k] = conn_info.get(k)
        if k == 'timestamp':
            sign_params[k] = timestamp
    # sign_str = '&'.join(sign_params.values())
    # sign = hashlib.sha256(sign_str.encode('utf-8')).hexdigest()
    # sign_params['sign'] = sign
    # sign_params_list = [f"{k}={v}" for k, v in sign_params.items()]
    # data_url_list = [f"{base_url}{api_url}?" + "&".join(sign_params_list)]
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
        'auth_func']
    print(f"-------------auth_func:{auth_func}------------")
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
        print(f"preset_params:{preset_params}")
        for param_key, param_value in api_source_params.items():
            if not param_value and param_key in conn_info.keys():
                api_source_params[param_key] = conn_info.get(param_key)
            if not param_value and param_key not in conn_info.keys() and \
                    (param_key in preset_params.keys() or param_key.lower() in preset_params.keys()):
                api_source_params[param_key] = preset_params[param_key]
        sign_params_list = [f"{k}={v}" for k, v in api_source_params.items()]
        data_url_list = [f"{base_url}{api_url}?" + "&".join(sign_params_list)]

    # elif auth_func == 'get_sign_by_encrypt_seq_params':
    #     if entity == 'wechatcustomer':
    #         all_api_params = []
    #         for cw_id in permissionRole['CWID']:
    #             sign_params_list = get_sign_by_encrypt_seq_params(api_source_params, conn_info, cw_id)
    #             all_api_params.append(sign_params_list)
    #         data_url_list = [f"{base_url}{api_url}?" + "&".join(api_params) for api_params in all_api_params]

    else:
        raise Exception("The authorization method is unsupported, please check configuration!")

    request_method = api_source_conf.get('method')
    post_offset = ""
    if request_method == 'POST':
        if load_mode in ('customized', 'full'):
            post_contents = {
                "endTime":"{{strftime('%Y-%m-%d 00:00:00', 'now','3 days')}}",
                "pageNum": 1,
                "pageSize": 1000,
                "startTime": "{{strftime('%Y-%m-%d 00:00:00', 'now','-3 days')}}",
            }
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

    return data_url_list[0]
    # return data_url_list[0]

load_mode = 'customized'
conn_info = {"sign": "7071d2b53ded4fe3a88f0e3014e3839e",
             "base_url": "https://bayer.wefeng360.com/api/v1",
             "appId": "ae5742d5-7845-4cbe-6946-a7ed80bf8558",
             "secret": "secret-f9885d51-db17-42a8-4d5b-9813e35ab3ce",
             "appSignKey": "sfBewHMYn1knLtu1eL7DsVXST3_PtbF2-4BVSD4Elk0"}
_alias = {
    "CURRENT_TIME": "time('now')",   # 09:33:18
    "CURRENT_TIMESTAMP": "unixepoch('now')",   # 1675848798
    "CURRENT_DATE": "strftime('%Y%m%d', 'now')",  # 2023-02-08
    "CURRENT_DATETIME": "strftime('%Y%m%d%H%M%S', 'now')",   # 2023-02-08 09:33:18
}
headers = {
    'Content-Type': 'application/json'
}

# data = {
#     "pageNum": 1,
#     "pageSize": 500,
#     "bizRecordTempId": 191
# }
result_list = []
url = get_auth_info()
print(url)
# def convert_data_type(data_list):
#     if data_list:
#         data_df = pd.DataFrame(data_list)
#         data_df = data_df.fillna('')
#         for col in data_df.columns:
#             data_df[col] = data_df[col].apply(lambda x: None if x in ('None', 'nan', 'NULL') else x)
#         data_list = data_df.to_dict('records')
#     return data_list
#
# def dynamic_post(url, headers, post_body_type, post_body, proxies=None, retry=10, interval=5):
#     while retry > 0:
#         try:
#             if not post_body:
#                 resp = requests.get(url, headers=headers, proxies=proxies)
#             else:
#                 resp = requests.post(url, headers=headers, data=post_body, proxies=proxies) \
#                     if post_body_type.lower() != "json" else \
#                     requests.post(url, headers=headers, json=post_body, proxies=proxies)
#             return resp
#         except Exception:
#             retry -= 1
#             time.sleep(interval)
#     raise Exception(f"Post failed of url: {url} after {retry} retries")
# # response = requests.request('post', data_url_list[0], headers=headers,data=json.dumps(data))

# while True:
#     response = dynamic_post(url, headers, "json", data, None).json()
#     pages_data_list = response.get("data")
#
#     # print(pages_data_list)
#     if pages_data_list:
#         pages_data_list = convert_data_type(pages_data_list)
#         result_list.extend(pages_data_list)
#         print(response.get("paging").get("pageNum"))
#         print(f"取了多少条数据:{result_list.__len__()}")
#         if response.get('code') == 400 and response.get('message') == "validate.api.timeout.error":
#             print("超时了")
#             url = get_auth_info()
#             response = dynamic_post(url, headers, "json", data, None).json()
#         if result_list.__len__() == response.get("paging").get("total"):
#             print("数据取完了")
#             break
#
#     else:
#         print("数据没有取完")
#         break
#     data["pageNum"] = int(data.get("pageNum")) + 1


# response = dynamic_post(data_url_list[0], headers, "json", data, None).json()
# print((response))
