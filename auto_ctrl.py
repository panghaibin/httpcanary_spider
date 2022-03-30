import os
import subprocess
from time import sleep


def click_product():
    subprocess.check_output("adb shell input tap %s %s" % (0x000002a2, 0x00000477))


def click_comment():
    subprocess.check_output("adb shell input tap %s %s" % (0x000003b7, 0x00000601))


def click_http():
    subprocess.check_output("adb shell input tap %s %s" % (0x00000397, 0x000001c8))
    sleep(0.5)
    subprocess.check_output("adb shell input tap %s %s" % (0x000003f2, 0x00000455))
    sleep(1)
    subprocess.check_output("adb shell input tap %s %s" % (0x000003f2, 0x00000455))
    sleep(0.5)
    subprocess.check_output("adb shell input swipe %s %s %s %s %s" % (0x000002c7, 0x00000091, 0x000003c7, 0x00000070, 300))


def click_return():
    subprocess.check_output("adb shell input keyevent 4")


def swipe_down():
    subprocess.check_output("adb shell input swipe %s %s %s %s %s" % (0x00000266, 0x00000839, 0x0000023e, 0x00000728, 100))
    subprocess.check_output("adb shell input tap %s %s" % (0x0000023e, 0x00000457))


def main():
    while True:
        key = input("请输入指令：")
        if key == "1":
            click_product()
        elif key == "2":
            click_comment()
        elif key == "3":
            click_http()
        elif key == "4":
            click_return()
        elif key == "5":
            swipe_down()
        else:
            print("输入错误，请重新输入")


def auto_ctrl():
    i = 0
    while True:
        i += 1
        click_product()
        sleep(5)
        click_comment()
        sleep(5)
        if i % 1 == 0:
            click_http()
            sleep(1)
        click_return()
        sleep(0.3)
        click_return()
        sleep(0.3)
        swipe_down()
        sleep(1)


if __name__ == '__main__':
    auto_ctrl()
    # swipe_down()
