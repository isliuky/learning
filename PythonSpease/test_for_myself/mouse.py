import pyautogui,time,random

while True:
    pyautogui.moveRel(0, -100, duration=1)
    pyautogui.moveRel(0, 100, duration=1)
    pyautogui.moveRel(100, 0, duration=1)
    pyautogui.moveRel(-100, 0, duration=1)
    pyautogui.click()
    b = random.randint(0, 9)
    print(b)
    time.sleep(b)

