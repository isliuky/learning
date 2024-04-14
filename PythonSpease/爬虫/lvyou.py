# -*- coding: utf-8 -*-
import requests
import io
from bs4 import BeautifulSoup as BS
import time
import re

"""从网上爬取数据"""

headers = {
    "Origin": "https://piao.ctrip.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
}
places = ["beijing1", "shanghai2", "changsha148", "sanya61", "chongqing158", "hongkong38", "chengdu104", "haerbin151",
          "xian7", "guangzhou152", "hangzhou14"]
placenames = ["北京", "上海", "长沙", "三亚", "重庆", "香港", "成都", "哈尔滨", "西安", "广州", "杭州"]

places = ["changsha148"]
placenames = ["长沙"]

base = "https://you.ctrip.com/fooditem/";
base2 = "https://you.ctrip.com";
requestlist = []

for j in range(len(places)):  # 爬取对应的特色菜
    requestlist.append({"url": base + places[j] + ".html", "place": placenames[j]})
    for i in range(2, 2):
        tmp = base + places[j] + "/s0-p" + str(i) + ".html"
        requestlist.append({"url": tmp, "place": placenames[j]});
# 对应的url地址和所查询的位置
print(requestlist)
l = []
count = 1;
for i in range(len(requestlist)):
    response = requests.get(requestlist[i]["url"], headers=headers)
    # print(response)
    html = response.text
    # print(html)
    soup = BS(html, 'html.parser')
    vs = soup.find_all(name="div", attrs={"class": "rdetailbox"})
    print("len(vs)", len(vs))
    for j in range(len(vs)):
        print("正在打印的条数:", j)
        try:
            # 获取子网页链接地址
            href = vs[j].find(name="a", attrs={"target": "_blank"}).attrs["href"];

            # print("href",href)
            # 再次请求子网页，获取景点详细信息
            res = requests.get(base2 + href, headers=headers)
            print("当前访问的网址：", base2 + href)
            with open("3.html", "w", encoding="utf-8") as f:
                f.write(res.text)
            soupi = BS(res.text, "html.parser")  # 该网页的html代码
            # print(soupi)
            vis = soupi.find_all(name="li", attrs={"class": "infotext"});  # 获取此时的dom文件位置所在
            # print(vis)
            introduce = []
            for i in range(len(vis)):
                introduce.append(vis[i].get_text())
            imgs = [];
            imglinks = soupi.find_all(name="a", attrs={"href": "javascript:void(0)"})
            # print(imte)
            # print(imglinks)
            # print(type(imglinks))
            # for img in imte:
            # imgs.append(img.attrs["src"])
            tmp = {};
            tmp["id"] = count;
            tmp["name"] = vs[j].find(name="a", attrs={"target": "_blank"}).string;
            tmp["name"] = tmp["name"].replace(" ", "").replace("\n", "");
            tmp["introduce"] = introduce
            tmp["img"] = imglinks
            tmp["city"] = requestlist[i]["place"]
            count = count + 1;
            l.append(tmp);
            time.sleep(1);
        except Exception as e:
            print(e)
            pass
        print ("打印tmp",tmp)
        # with open("datap/"+tmp["name"]+".pk",'wb') as f:
        # 	pickle.dump(tmp,f);

        # with io.open("/Users/hujinhong/PythonProjects/untitled5/food/changsha/" + tmp["name"] + ".txt", 'w',
        #              encoding="utf-8") as f:
        #     f.write(str(tmp))
# print(l)
for i in l:
    print((i))