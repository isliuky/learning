# import requests
#
# url = "https://dnfqbtu4va.execute-api.cn-north-1.amazonaws.com.cn/dev/query?database=cn_cdp_dev&schema=analytical_appddm_mob&table=mob_activity_kpi"
#
# payload = {}
# headers = {
#   'x-api-key': 'jY2pDEjCR0azvOMOOa4oc8ISHdvzQDQ18hqraVix',
#   'Authorization': 'Basic dGVzdF91c2VybmFtZTp0ZXN0X3Bhc3N3b3Jk'
# }
#
# response = requests.request("GET", url, headers=headers, data=payload)
#
# print(response.text)
#
# headers = {'User-Agent': 'Mozilla/5.0'}  # 设置请求头
# params = {'key1': 'value1', 'key2': 'value2'}  # 设置查询参数
# data = {'username': 'example', 'password': '123456'}  # 设置请求体
# response = requests.post('https://www.runoob.com', headers=headers, params=params, data=data)
# print(response.text)
import requests

url = "http://www.baidu.com"

a = requests.get(url)
print(a.text)