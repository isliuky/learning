import requests
from bs4 import BeautifulSoup
from pypinyin import slug

# addres_input = input(f"请输入你想获取的地址:")
# addres = slug(addres_input, separator='')
# print(addres)
# url = f'https://www.douban.com/group/{addres}/'
base_url = f'https://www.douban.com/group/shanghaizufang/discussion?start=400&type=new'
# print(url)
'https://www.douban.com/group/beijingzufang/discussion?start=0&type=new'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

response = requests.get(base_url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')

# print(soup)
# 获取该页面的数据
# house_list = soup.findAll("tr")
#
# for house in house_list:
#     print(house.get_text())
#     for i in house.contents:
#         # todo: 获取元素的所有格式
#         # print(repr(i))
#         print("=======")
#         print(i)
#         if i.string == None:
#             continue
#         elif i.string.replace("\n", "") == "":
#             continue
#         else:
#             print(i.string)
#             # print(i)

# todo 方式一 获取超链接及title
# house_list = soup.findAll("td",class_="title")
# for house in house_list:
#     print(house)
#     a_tag = house.find('a')
#     print(a_tag)
#     print(a_tag["href"],a_tag["title"])

# todo 方式二 获取超链接及title
# def download_all_htmls():
#     htmls = []
#     for baseUrl in baseUrls:
#         for idx in page_indexs:
#
#             UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
#             url = f"{baseUrl}?start={idx}"
#             print("download_all_htmls craw html:", url)
#             r = requests.get(url,
#                             headers={"User-Agent":UA,"Cookie":cookie})
#             if r.status_code != 200:
#                 print('download_all_htmls,r.status_code',r.status_code)
#             htmls.append(r.text)
#     return htmls

items = soup.find("table",class_='olt')
if items == None:
    print("shibai")
else:
    itemss =items.findAll("tr",class_="")
    for i in itemss:
        print(i)