import requests
from lxml import etree


headers = {'User-Agent:', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'}
rep = requests.get('https://nba.hupu.com/stats/players')

e = etree.HTML(rep.text)
nbames = e.xpath('//table[@class="players_table"]//tr/td[2]/a/text()')
print(nbames)