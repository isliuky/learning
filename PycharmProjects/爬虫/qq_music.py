import requests
from bs4 import BeautifulSoup

# 热门歌曲榜单 URL
url = 'https://y.qq.com/n/ryqq/toplist/26'

# 发送 HTTP 请求，获取网页源代码
response = requests.get(url)
html = response.text
# print(html)

# 解析 HTML，提取歌曲信息
soup = BeautifulSoup(html, 'html.parser')
# for link in soup.find_all('a'):
#     print(link.get('href'))
#

song_list_all = soup.select('div.songlist__songname > span> a')
print(song_list_all)
for song in song_list_all:
    print(song)
song_list_all = soup.select('div',class_='songlist__songname')
# print(song_list_all)
for song in song_list_all:
    print(song)
# for song in song_list_all:
#     song_name = song.select_one('a').text
#     song_title = song.select_one('a').get('title')
#     print(song_name, song_title)
#



# for song in song_list:
#     # 歌曲名
#     name = song.find('a', class_='js_songname')['title']
#     # 歌手
#     singer = song.find('a', class_='js_singer')['title']
#     # 专辑
#     album = song.find('a', class_='js_album')['title']
#     # 播放链接
#     play_link = 'https://y.qq.com/n/yqq/song/' + song['data-id'] + '.html'
#
#     # 输出歌曲信息
#     print('歌曲名：', name)
#     print('歌手：', singer)
#     print('专辑：', album)
#     print('播放链接：', play_link)
#     print('----------------------------------------')
