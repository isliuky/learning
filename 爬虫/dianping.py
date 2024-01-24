import requests
from bs4 import BeautifulSoup

url = 'https://www.mafengwo.cn/'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
response = requests.get(url, headers=headers)
html = response.text

soup = BeautifulSoup(html, 'html.parser')
city_list = soup.find_all('span', {'class': 'tn'})
for city in city_list:
    city_name = city.text
    city_url = city.parent['href']
    city_id = city_url.split('/')[-2]
    city_poi_url = 'https://www.mafengwo.cn/jd/%s/gonglve.html' % city_id
    city_response = requests.get(city_poi_url, headers=headers)
    city_html = city_response.text
    city_soup = BeautifulSoup(city_html, 'html.parser')
    spot_list = city_soup.find_all('a', {'class': 'title'})
    for spot in spot_list:
        spot_name = spot.text
        spot_url = 'https://www.mafengwo.cn%s' % spot['href']
        spot_response = requests.get(spot_url, headers=headers)
        spot_html = spot_response.text
        spot_soup = BeautifulSoup(spot_html, 'html.parser')
        comment_list = spot_soup.find_all('div', {'class': 'rev-item comment-item'})
        for comment in comment_list:
            comment_text = ''
            if comment.find('div', {'class': 'rev-txt'}):
                comment_text = comment.find('div', {'class': 'rev-txt'}).text.strip()
            elif comment.find('div', {'class': 'rev-txt-all'}):
                comment_text = comment.find('div', {'class': 'rev-txt-all'}).text.strip()
            if comment_text:
                print(city_name, spot_name, comment_text)
