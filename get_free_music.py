# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import urllib2
import time
import random
import threading

# 设置UA等headers和主机名
UA = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
HEADERS = {'User-Agent': UA}
HOST = 'https://www.tikitiki.cn/'


# 获取网页中各条音乐的信息，将其以{'title':title,'src':src}的形式存入list中并返回
def get_music_info(url):
    music_info = []
    r = requests.get(url, headers=HEADERS)
    response = r.text
    soup = BeautifulSoup(response, "html.parser")
    html = soup.find_all("div", class_="mdui-panel-item")

    for item in html:
        title = item.find("div", class_="Stitle").get_text()
        body = item.find("div", class_="mdui-panel-item-body")
        highQuality = body.find("a", class_="item_lable_320")
        lowQuality = body.find("a", class_="item_lable_128")
        src = ''
        if (highQuality != None):
            title += '_320k'
            src = HOST + highQuality.get('href')
        elif (lowQuality != None):
            title += '_128k'
            src = HOST + lowQuality.get('href')
        music_info.append({'title': title, 'src': src})

    return music_info


# 将音乐保存到本地
def save_music(url, title):
    request = urllib2.Request(url, headers=HEADERS)
    music = urllib2.urlopen(request).read()
    with open(title + '.mp3', "wb") as file:
        file.write(music)
    print('done:' + title)


# 开启下载线程，每调用一次随机sleep 0到5秒避免检测(虽然并不知道有没有检测)
def download_thread(item):
    print('start:' + item['title'])
    save_music(item['src'], item['title'])
    time.sleep(round(random.random() * 5, 2))


if __name__ == "__main__":
    url = raw_input('input your address: ')
    t0 = time.time()
    threads = []
    music_info = get_music_info(url)
    print('total: ' + str(len(music_info)))
    # with threads
    for item in music_info:
        thread = threading.Thread(target=download_thread, args=(item,))
        threads.append(thread)
        thread.start()
    for item in threads:
        item.join()
    t1 = time.time()
    print('ALL DONE')
    print('spend: ' + str(t1 - t0))
