import math
import time
import pyautogui
import keyboard
import threading
import json

# ------------ 配置 -----------------
# 读取 JSON 文件配置
with open('config.json', 'r', encoding='utf-8') as file:
    data = json.load(file)
change_sleep = data.get("change_sleep", 6.5)
map_point1 = data.get("map_point1", [[846, 321], [846, 365], [846, 420], [846, 487], [846, 543]])
map_point2 = data.get("map_point2", [[1010, 333], [1010, 403], [1010, 473]])
home = data.get("home", [1136, 817])
_map = [990, 460]
point = data.get("point", [961, 795])
master1 = data.get("master1", 1)
master2 = data.get("master2", 3)
master3 = data.get("master3", 1.5)
master4 = data.get("master4", 1)
restart_time = data.get("restart_time", 3.5)
click_empty = data.get("click_empty",0)
restart_point = data.get("restart_point", [[1082, 117], [950, 368]])


# -------------- 配置结束------------

# 点击鼠标滑动
def slide(x, y, x1, y1, duration):
    pyautogui.moveTo(x, y)
    # 模拟按住左键
    pyautogui.mouseDown(button='left')
    # 移动鼠标，这里只是一个示例，你可以将坐标调整为你想要的位置
    pyautogui.moveTo(x1, y1, duration)
    # 松开左键
    pyautogui.mouseUp(button='left')


def click(x, y):
    pyautogui.click(x, y)


def next_coordinate(x, y, angle_degrees):
    # 长度
    h = 50
    angle_radians = math.radians(angle_degrees)
    next_x = x + h * math.cos(angle_radians)
    next_y = y + h * math.sin(angle_radians)
    return next_x, next_y


# 移动
def go(random_angle, time):
    x1, y1 = next_coordinate(*point, random_angle)
    slide(*point, x1, y1, time)


# 回家
def goHome():
    pyautogui.click(*home)
    time.sleep(change_sleep)


def restart():
    global _map
    pyautogui.click(*restart_point[0])
    time.sleep(0.3)
    pyautogui.click(restart_point[1])
    time.sleep(restart_time)
    # 从新进入地图 位置会变
    _map = data.get("_map2", [1001, 409])
    # 新手礼包需要点击空白处跳过
    if click_empty != 0:
        print("点击空白处")
        click(home[0],home[1] - 100)
        time.sleep(click_empty)


def jump(p1, p2):
    global _map
    # 点击地图
    click(*_map)
    time.sleep(0.2)
    # 跳转地图
    click(*map_point1[p1 - 1])
    time.sleep(0.2)
    click(*map_point2[p2 - 1])
    time.sleep(change_sleep)
    _map = data.get("_map", [990, 460])


def m1_1():
    jump(2, 1)
    go(350, 1.8)
    time.sleep(master1)
    go(170, 1.8)


def m1_2():
    jump(3, 3)
    go(0, 1.6)
    go(310, 2)
    time.sleep(master2)
    go(130, 2)
    go(180, 1.6)


def m1_3():
    jump(2, 2)
    go(270, 0.5)
    go(225, 3)
    go(285, 6)
    go(225, 1.5)
    time.sleep(master3)


def m1_4():
    go(225, 1)
    go(270, 6)
    time.sleep(master4)


run_statu = False


def wait():
    global run_statu
    # 保证打印一次
    flag = True
    while not run_statu:
        if flag:
            print("暂停脚本，等待...（按ctrl + f1  开始或暂停脚本）")
            flag = False
        time.sleep(0.2)
    flag = True


def playGame():
    global run_status
    # 如果在其他地图 先回家
    wait()
    goHome()
    print("开始刷金币")
    count = 0
    # 添加要打的boss
    actions = []
    if master1 != 0:
        actions.append(m1_1)
    if master2 != 0:
        actions.append(m1_2)
    if master3 != 0:
        actions.append(m1_3)
    if master4 != 0:
        actions.append(m1_4)
    while True:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        for action in actions:
            wait()
            action()
            count = count + 1
            print(f"action执行次数：{count}")
        # 回家然后传送去王座大厅 刷新boss时间
        wait()
        restart()


# 创建线程实例并启动线程
thread1 = threading.Thread(target=playGame)
thread1.start()


def on_press(event):
    global run_statu
    if event.event_type == keyboard.KEY_DOWN and keyboard.is_pressed('f1') and keyboard.is_pressed('ctrl'):
        run_statu = not run_statu
        if (run_statu):
            print("开始执行脚本!")
        else:
            print("暂停脚本,等待上一步动作完成后可接管鼠标!")


def thread2():
    # 设置监听器
    keyboard.hook(on_press)
    # 进入监听状态
    keyboard.wait()


# 创建线程
my_thread2 = threading.Thread(target=thread2)
# 启动线程
my_thread2.start()
my_thread2.join()