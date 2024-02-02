import requests
from bs4 import BeautifulSoup

# 设置请求头，模拟浏览器访问
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}


for moive_no in range(0, 250, 25):
    # 发送GET请求
    url = f'https://movie.douban.com/top250?start={moive_no}&filter='
    res = requests.get(url, headers=headers).text
    soup = BeautifulSoup(res, "html.parser")
    titles = soup.findAll("span", attrs={"class": "title"})
    for title in titles:
        if title.get_text().startswith(" / "):
            continue
        else:
            print(title.get_text())
