import requests
from bs4 import BeautifulSoup

# 设置请求头，模拟浏览器访问
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

# 发送GET请求
url = 'https://books.toscrape.com/'
res = requests.get(url, headers=headers).text
soup = BeautifulSoup(res, "html.parser")
# price = soup.findAll("p", attrs={"class":"price_color"})
# for i in price:
#     print(i.string[2:])
book_names = soup.findAll("h3")
for book_name in book_names:
    print(book_name.a.get_text())