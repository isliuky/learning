import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random

# 起始条目，最终条目，每页条数
page_indexs = range(0, 250, 25)

# 租房小组链接
baseUrls = ['https://www.douban.com/group/szsh/discussion',  # 深圳租房
            'https://www.douban.com/group/106955/discussion'  # 深圳租房团
            ]

# cookie，注意
cookie = 'bid=ipVYZlUpAf0; _pk_id.100001.8cb4=6c05a9ff3c70daca.1706165630.; ap_v=0,6.0; __utmc=30149280; __utmz=30149280.1706165631.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __yadk_uid=WSDIw9M7mTMbO5X96YsO8EiOhvxVwj9d; _pk_ses.100001.8cb4=1; __utma=30149280.412457508.1706165631.1706165631.1706170462.2; __utmt=1; __utmb=30149280.29.4.1706171100111'


# # 下载每个页面
# def download_all_htmls():
#     htmls = []
#     for baseUrl in baseUrls:
#         for idx in page_indexs:
#
#             UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
#             url = f"{baseUrl}?start={idx}"
#             print("download_all_htmls craw html:", url)
#             r = requests.get(url,
#                              headers={"User-Agent": UA, "Cookie": cookie})
#             if r.status_code != 200:
#                 print('download_all_htmls,r.status_code', r.status_code)
#             htmls.append(r.text)
#     return htmls
#
#
# htmls = download_all_htmls()
# print(htmls)
#
# # 保存每个标题名称，以便后续去重
# datasKey = []
#
#
# # 解析单个HTML，得到数据
# def parse_single_html(html):
#     soup = BeautifulSoup(html, 'html.parser')
#
#     article_items = (
#         soup.find("table", class_="olt")
#             .find_all("tr", class_="")
#     )
#
#     datas = []
#
#     for article_item in article_items:
#         print(article_item)
#     #     # 文章标题
#     #     title = article_item.find("td", class_="title").get_text().strip()
#     #     # 文章链接
#     #     link = article_item.find("a")["href"]
#     #     # 文章时间
#     #     time = article_item.find("td", class_="time").get_text()
#     #
#     #     # 匹配科技园、竹子林、车公庙三个关键字
#     #     res1 = re.search("科技园|竹子林|车公庙", title)
#     #     # 筛选一房
#     #     res2 = re.search("一房|单间|一室|1房|1室", title)
#     #
#     #     # 找到地点和一房匹配的标题和之前存储的列表中不存在的
#     #     if res1 is not None and res2 is not None and not title in datasKey:
#     #         print(title, link, time)
#     #         datasKey.append(title)
#     #         datas.append({
#     #             "title": title,
#     #             "link": link,
#     #             "time": time
#     #         })
#     # return datas
#
#
# all_datas = []
#
# # 遍历所有爬取到的html，并解析
# for html in htmls:
#     all_datas.extend(parse_single_html(html))
#
# # df = pd.DataFrame(all_datas)
# # # 将数据转成Excel
# # df.to_excel("test.xlsx")

time.sleep(5)
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'
    }
base_url = f'https://www.douban.com/group/shanghaizufang/discussion?start=400&type=new'
response = requests.get(base_url, headers=headers)
if response.status_code != 200:
    print("shibai"+str(response.status_code))
datasKey = []
datas = []
soup = BeautifulSoup(response.text, 'html.parser')
article_items = (
    soup.find("table", class_="olt")
        .find_all("tr", class_="")
)
for article_item in article_items:
    # print(article_item)
    # 文章标题
    title = article_item.find("td", class_="title").get_text().strip()
    # 文章链接
    link = article_item.find("a")["href"]
    # 文章时间
    time = article_item.find("td", class_="time").get_text()
    # 作者链接
    pepole_link = article_item.find("td", nowrap="nowrap").find('a')["href"]

    pepole_name = article_item.find("td", nowrap="nowrap").find('a').text.strip()
    print(title,link,time,pepole_link,pepole_name)

    # 匹配科技园、竹子林、车公庙三个关键字
    res1 = re.search("科技园|竹子林|车公庙", title)
    # 筛选一房
    res2 = re.search("一房|单间|一室|1房|1室", title)

    # 找到地点和一房匹配的标题和之前存储的列表中不存在的
    if res1 is not None and res2 is not None and not title in datasKey:
        print(title, link, time)
        datasKey.append(title)
        datas.append({
            "title": title,
            "link": link,
            "time": time
        })
print(datas)
print(datasKey)
