from wxpusher import WxPusher
import requests
app_token = "AT_ZDHrX4mSo9ognUoF2wlsRHOZWvcTzm6Z"
uids = ['UID_XJIFht0Y1g3tUygmEJ6yzXIc04h0']
dict = {"title":"个人出租，8号线，杨思地铁站，价格2000元，精致装修，配置齐.","url":" https://www.douban.com/group/topic/300916583/"}
data = f"""
<!DOCTYPE HTML>

<html>
	<head>
		<title>这个是一的demo</title>
	</head>	
	<body>
		<a  href = "https://www.douban.com/group/topic/300916583/">个人出租，8号线，杨思地铁站，价格2000元，精致装修，配置齐.</a>
	</body>
</html>

"""
# a = WxPusher.send_message(f'{data}',
#                       uids=uids,
#                       token=app_token)
# print(a)
# WxPusher.query_message('<messageId>')
# WxPusher.create_qrcode('<extra>', '<validTime>', '<appToken>')
# WxPusher.query_user('1', '10', app_token)
# header = {"Content-Type":"application/json"}
# data = {
#   "appToken":app_token,
#   "content":"Wxpusher祝你中秋节快乐!",
#   "summary":"消息摘要",
#   "contentType":1,6
#   "uids":uids,
#   "verifyPay":"false"
# }
# print(data)
# resp = requests.post(url="https://wxpusher.zjiecode.com",headers=header, data=data)
# print(resp.status_code)
#
#
payload = {
  'appToken': app_token,
  'content': f'{data}',
  'contentType':2,
  'uids': uids,
  'url': "https://wxpusher.zjiecode.com",
}
BASEURL = 'http://wxpusher.zjiecode.com/api'
url = f'{BASEURL}/send/message'
a = requests.post(url, json=payload).json()
print(a)