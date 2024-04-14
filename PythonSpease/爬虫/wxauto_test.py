# from wxauto import WeChat
#
# # 获取当前微信客户端
# wx = WeChat()
#
#
# # 获取会话列表
# wx.GetSessionList()
#
# # 向某人发送消息（以`文件传输助手`为例）
# # msg = '你好~'
# # who = '文件传输助手'
# # wx.SendMsg(msg, who)  # 向`文件传输助手`发送消息：你好~
#
#
# #  # 向某人发送文件（以`文件传输助手`为例，发送三个不同类型文件）
# # files = [
# #     'D:/test/wxauto_test.py',
# #     'D:/test/pic.png',
# #     'D:/test/files.rar'
# #  ]
# # who = '文件传输助手'
# # wx.SendFiles(filepath=files, who=who)  # 向`文件传输助手`发送上述三个文件
#
#
# # # 下载当前聊天窗口的聊天记录及图片
# # msgs = wx.GetAllMessage(savepic=True)   # 获取聊天记录，及自动下载图片
# wx.SwitchToChat()
# wx.SwitchToContact()
# # wx.GetGroupMembers()
# wx.GetAllFriends()

from wxauto import *
import time

# 实例化微信对象
wx = WeChat()

# 指定监听目标
listen_list = [
    '老王'
]
for i in listen_list:
    wx.AddListenChat(who=i, savepic=True)  # 添加监听对象并且自动保存新消息图片

# 持续监听消息，并且收到消息后回复“收到”
wait = 10  # 设置10秒查看一次是否有新消息
while True:
    msgs = wx.GetListenMessage()
    for chat in msgs:
        msg = msgs.get(chat)  # 获取消息内容
        # ===================================================
        # 处理消息逻辑
        #
        # 处理消息内容的逻辑每个人都不同，按自己想法写就好了，这里不写了
        #
        # ===================================================

        # 回复收到
        chat.SendMsg('收到')  # 回复收到
    time.sleep(wait)
