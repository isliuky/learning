import requests
from bs4 import BeautifulSoup
import re
import logging
import pandas as pd

# 配置日志
logging.basicConfig(level=logging.INFO,  # 设置日志级别为INFO，可以选择DEBUG、WARNING等级别
                    format='%(asctime)s - %(levelname)s - %(message)s')

# todo 需要接受的参数：
#     1. 城市
#     2. 想租的地铁线及位置
#     3. 整租合租
#     4. 一房，二房，三房

# 获取参数
# 多选框
want_city = ["上海","北京"]

want_addrs = ["12号线","闵行","七宝"]
want_addrs_pattern = '|'.join(want_addrs)
room_type = [ '1房', '一室','一房','1室']
room_type_pattern = '|'.join(room_type)
print(want_addrs_pattern,room_type_pattern)
# cookie
cookie = 'bid=ipVYZlUpAf0; _pk_id.100001.8cb4=6c05a9ff3c70daca.1706165630.; ap_v=0,6.0; __utmc=30149280; __utmz=30149280.1706165631.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __yadk_uid=WSDIw9M7mTMbO5X96YsO8EiOhvxVwj9d; _pk_ses.100001.8cb4=1; __utma=30149280.412457508.1706165631.1706165631.1706170462.2; __utmt=1; __utmb=30149280.29.4.1706171100111'
# headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'
    }
# 先给一个base_url
base_url = f'https://www.douban.com/group/shanghaizufang/'

# 起始条目，最终条目，每页条数
page_indexs = range(0, 250, 25)
# 将数据添加到列表里面
datas = []
his_datas = []
# 开始
response = requests.get(base_url, headers=headers)
if response.status_code != 200:
    # 对状态码进行判断，如果有问题就抛出异常
    logging.error(f"返回的状态码为{response.status_code}")
else:
    # 解析
    soup = BeautifulSoup(response.text, 'html.parser')
    # 抓取页面table下面的tr
    article_items = soup.find("table", class_="olt").find_all("tr", class_="")
    #遍历
    for article_item in article_items:
        # 文章标题
        title = article_item.find("td", class_="title").get_text().strip()
        # 文章链接
        link = article_item.find("a")["href"]
        # 文章时间
        time = article_item.find("td", class_="time").get_text()
        # 作者链接
        pepole_link = article_item.find("td", nowrap="nowrap").find('a')["href"]
        # 作者名字
        pepole_name = article_item.find("td", nowrap="nowrap").find('a').text.strip()
        print(title,link,time,pepole_link,pepole_name)
        # 匹配关键字
        want_addrs = re.search(want_addrs_pattern, title)
        room_type = re.search(room_type_pattern, title)

        # 找到地点和一房匹配的标题和之前存储的列表中不存在的
        if want_addrs is not None and room_type is not None and title not in his_datas:
            his_datas.append(title)
            datas.append({
                "title": title,
                "link": link,
                "time": time
            })
data_df = pd.DataFrame(datas)
his_data_df = pd.DataFrame(his_datas)
# 将数据转成Excel
data_df.to_excel("datas.xlsx")
his_data_df.to_excel("his_datas.xlsx")
print(datas)
print(his_datas)
