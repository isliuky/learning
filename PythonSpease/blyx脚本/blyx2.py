import time
import win32gui
import pyautogui
import json
import math
import keyboard
import threading


# 查找游戏窗口，返回窗口起始坐标
def find_flash_window():
    hwnd = win32gui.FindWindow(None, "百炼英雄")
    if (hwnd):
        rect = win32gui.GetWindowRect(hwnd)
        win32gui.SetForegroundWindow(hwnd)
        return rect
    return None


def getSource(x, y):
    x_source = x * length + x_start
    y_source = y * breadth + y_start
    return x_source, y_source


# 获取窗口
window = find_flash_window()
# x 开始
x_start = window[0]
# x 结束
x_end = window[2]
# y 开始
y_start = window[1]
# y 结束
y_end = window[3]
# 长
length = x_end - x_start
# 宽
breadth = y_end - y_start

# 以后要设置成参数
map_point1 = [getSource(0.23, 0.278), getSource(0.23, 0.344), getSource(0.23, 0.403), getSource(0.23, 0.462),
              getSource(0.23, 0.520)]
map_point2 = [getSource(0.635, 0.3), getSource(0.635, 0.388), getSource(0.635, 0.462)]
restart_point = [getSource(0.59, 0.03), getSource(0.29, 0.40)]
# 系统是否会弹出新手礼包
click_empty = 0
point = getSource(0.5, 0.813)
home = getSource(0.9, 0.872)
run_statu = False
_map = getSource(0.575, 0.447)
master1 = 0.8
master2 = 3
master3 = 1
master4 = 0
master5 = 2
# 以后要设置成参数,回家之后需要等待英雄传送的时间，人数越少可以设置越小
change_sleep = 5


# 回家
def goHome():
    pyautogui.click(*home)
    time.sleep(change_sleep)


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
    # 以后要设置成参数 ，跟 回家之后的 map 保持一致
    _map = getSource(0.575, 0.447)


def restart():
    global _map
    pyautogui.click(*restart_point[0])
    time.sleep(0.3)
    pyautogui.click(restart_point[1])
    # 等待游戏加载的时间，这个是重启小游戏等待游戏加载的时间
    time.sleep(5)
    # 从新进入地图 位置会变 这个需要重新配置
    _map = getSource(0.59, 0.38)
    # 新手礼包需要点击空白处跳过
    if click_empty != 0:
        print("点击空白处，不购买新手礼包，直接跳过")
        time.sleep(click_empty)
        click(*getSource(0.83, 0.97))


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


def go(random_angle, time):
    x1, y1 = next_coordinate(*point, random_angle)
    slide(*point, x1, y1, time)


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


def m1_1():
    jump(2, 1)
    go(350, 1.8)
    time.sleep(master1)
    go(170, 1.8)


def m1_2():
    jump(3, 3)
    go(0, 1.2)
    go(310, 1.5)
    time.sleep(master2)
    go(110, 1.2)
    go(180, 1.8)


def m1_3():
    jump(2, 2)
    go(270, 0.5)
    go(225, 3)
    go(285, 5.2)
    # go(225, 1.5)
    time.sleep(master3)


def m1_4():
    go(225, 1)
    go(270, 6)
    time.sleep(master4)


def m1_5():
    go(225, 1)
    go(270, 6.5)
    time.sleep(master5)


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
    if master5 != 0:
        actions.append(m1_5)
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
