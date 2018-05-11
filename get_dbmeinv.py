# -*- coding: utf-8 -*-
import sys
import requests
from bs4 import BeautifulSoup
import dateutil.parser
from firebase.firebase import FirebaseApplication
from firebase.firebase import FirebaseAuthentication
import time
import random
import logging

reload(sys)
sys.setdefaultencoding('utf-8')

# 设置headers
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36'
HEADERS = {'User-Agent': UA, 'Connection': 'close'}

# firebase配置
FIREBASE_SECRET = "你的firebase配置"
FIREBASE_URL = "你的firebase配置"
AUTH = FirebaseAuthentication(FIREBASE_SECRET, '你的firebase配置')
MYFIREBASE = FirebaseApplication(FIREBASE_URL, None)
MYFIREBASE.authentication = AUTH

# 爬取的类别，默认全部为空
# 2：大胸妹 3：美腿控 4：有颜值 5：大杂烩 6：小翘臀 7：黑丝袜
cid = ''
cid_dict = {
    '': 'all',
    2: 'breast',
    3: 'leg',
    4: 'face',
    5: 'hotpot',
    6: 'buttock',
    7: 'stockings'
}
# 页数，默认从1开始
pager_offset = 1

# 一些储存的参数
# 用户的id列表，用于去重
id_list = set()
# 首页的url list，用于遍历获取详情页数据
url_list = []
# 是否有用户id和firebase中的重复，如果重复说明爬到了之前爬过的内容，就结束本次爬取
has_repeat_get = False

# firebase错误计数器，如果报错则睡眠一下重试，超过三次则退出
firebase_post_error_count = 0


# 拼接请求的地址
def set_request_host():
    # 默认地址
    global HOST
    HOST = 'https://www.dbmeinv.com/dbgroup/show.htm'
    # 拼接地址
    HOST = HOST + '?pager_offset=' + str(pager_offset)
    # 如果有cid则在url中传入
    if cid != '':
        HOST = HOST + '&cid=' + cid


# 使用requests爬取网页的html代码，使用BeautifulSoup格式化之后返回格式化的代码
def get_format_site_html(url):
    try:
        r = requests.get(url, headers=HEADERS)
        logging.info(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())) + ': 请求结果  ' + str(r))
        soup = BeautifulSoup(r.text, "html.parser")
        time.sleep(2.5 + round(random.random() * 2, 1))
        return soup
    except Exception, e:
        logging.error(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())) + ': requests爬取url失败  ' + str(e))
        exit()


# 解析html提取出各个图片跳转的地址
def get_img_jump_url(div_list):
    for item in div_list:
        try:
            href = item.find('a')['href']
            _id = href.split('dbgroup/')[1]
            # 如果id在firebase中已经存在了，说明之前已经爬取过这些内容，那么就直接停止爬取
            # if _id in id_set_from_firebase:
            #     traverse_url_list()
            #     exit()
            # 否则判断页面中是否重复出现同个用户，去重之后遍历爬取
            if _id not in id_list:
                id_list.add(_id)
                url_list.append({"id": _id, "url": href})
        except Exception, e:
            logging.error(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())) + ': 提取跳转地址失败  ' + str(e))
            exit()


# 获取用户详情页面的数据
def get_user_page_info(url):
    logging.info('开始id为： ' + url + ' 的数据爬取')
    soup = get_format_site_html(url)
    media_body = soup.find_all('div', class_="media-body")[0]
    title = media_body.find(class_="media-heading").text
    date = media_body.find(class_="info").find("abbr").text
    date = dateutil.parser.parse(date)
    user_card = soup.find(class_="user-card")
    avatar = user_card.find(class_="img-circle")['src']
    info = user_card.find(class_="info")
    author_name = info.find(class_="name").text
    location = info.find(class_="loc").text.strip()
    panel_body = soup.find(class_="topic-detail").find(class_="panel-body")
    iframe = panel_body.find("iframe")
    if iframe:
        iframe.extract()
    content = get_all_content(panel_body)
    imgs = get_img_list(panel_body)
    href = soup.find('span', class_="mobile-hide").find("a")['href']
    _id = href.split('dbgroup/')[1]
    user_info_dict = {
        "authorName": author_name,
        "avatar": avatar,
        "cid": cid,
        "title": title,
        "date": str(date),
        "location": location,
        "content": content,
        "id": _id,
        "imgs": imgs,
    }
    logging.info('完成id为： ' + _id + ' 的数据爬取')
    return user_info_dict


# 获取image_container中的图片并返回
def get_img_list(panel_body):
    img_list = []
    imgs = panel_body.find_all("img")
    for item in imgs:
        src = item["src"]
        img_list.append(src)
    return img_list


# 获取页面中的文本内容
def get_all_content(panel_body):
    scripts = panel_body.find_all("script")
    for item in scripts:
        item.extract()
    body_text = panel_body.text.strip()
    final_text = ''
    for line in body_text.splitlines():
        strip_text = line.strip()
        if strip_text:
            final_text += strip_text + ';'
    return final_text


# 将数据写入firebase中
def send_data_to_firebase(key, data):
    try:
        MYFIREBASE.post(key, data)
    except Exception, e:
        global firebase_post_error_count
        firebase_post_error_count += 1
        if firebase_post_error_count < 3:
            time.sleep(5 + firebase_post_error_count * 2)
            send_data_to_firebase(key, data)
        else:
            firebase_post_error_count = 0
            logging.error(
                str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())) + ': 数据写入firebase中失败，key:' + str(key) + '  data:' + str(
                    data) + '   ' + str(e))
            exit()


# 从firebase中读取数据
def get_data_from_firebase(key):
    try:
        result = MYFIREBASE.get(key, None)
        return result
    except Exception, e:
        logging.error(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())) + ': 读取firebase数据失败  ' + str(e))
        exit()


# 删除firebase中的数据
def delete_data_from_firebase(key, index=None):
    try:
        MYFIREBASE.delete(key, index)
    except Exception, e:
        logging.error(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())) + ': 删除firebase数据失败  ' + str(e))
        exit()


# 从firebase中获取之前已经爬取过的用户id，避免重复爬取
def get_id_set_from_firebase():
    global id_set_from_firebase
    res = get_data_from_firebase('IdSet' + cid_dict[cid])
    id_set_from_firebase = set()
    # 返回的result类型为dict
    if res:
        for item in res.itervalues():
            id_set_from_firebase = set(item)


# 将最新的用户id更新到firebase中
def update_id_set_to_firebase():
    new_id_set = id_list | id_set_from_firebase
    # 删除firebase中的记录，之后再次上传
    delete_data_from_firebase('IdSet' + cid_dict[cid])
    send_data_to_firebase('IdSet' + cid_dict[cid], list(new_id_set))


# 遍历url_list，获取用户详情页面的数据
def traverse_url_list():
    for item in url_list:
        user_info_dict = get_user_page_info(item['url'])
        send_data_to_firebase(cid_dict[cid], user_info_dict)
        logging.info('完成地址为： ' + item['url'] + ' 的数据爬取保存')
    update_id_set_to_firebase()


# 入口方法，便于修改完最后一页的页数和cid之后复用
def start_get_data(_cid, last_page=0):
    global pager_offset
    global id_list
    global url_list
    global cid
    cid = _cid
    while pager_offset <= last_page:
        # 先从firebase中获取已经爬取过的用户id
        get_id_set_from_firebase()
        # 将id_list和url_list重置
        id_list = set()
        url_list = []
        # 爬取页面数据，将各个用户的信息去重以{"id":id,"url":url}的形式保存
        set_request_host()
        logging.info('爬取地址为： ' + HOST + ' 的数据')
        soup = get_format_site_html(HOST)
        img_single_list = soup.find_all('div', class_="img_single")
        get_img_jump_url(img_single_list)
        traverse_url_list()
        pager_offset += 1
        time.sleep(15 + round(random.random() * 10, 1))


if __name__ == "__main__":
    print 'start'
    logging.basicConfig(filename=time.strftime("%Y-%m-%d", time.localtime()) + '-logger.log', level=logging.INFO)
    # 全部
    start_get_data('', 3000)
