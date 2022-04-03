from data_clean import *
import json
import time
import csv


db = DatabaseUtils()
db_c = DataClean()

# %% 获取区域最多的用户
returned_customer_comment = db_c.filter_returned_customer_comment()
with open('returned_customer_comment.json', 'w', encoding='utf-8') as f:
    json.dump(returned_customer_comment, f, ensure_ascii=False, indent=2)

# %% 加载区域最多的用户
with open(r'returned_customer_comment.json', 'r', encoding='utf-8') as f:
    returned_customer_comment = json.load(f)
print(len(returned_customer_comment))
returned_customer_comment_count = {}
for user_hash in returned_customer_comment:
    returned_customer_comment_count[user_hash] = len(returned_customer_comment[user_hash])
# 排序
returned_customer_comment_count = sorted(returned_customer_comment_count.items(), key=lambda x: x[1], reverse=True)
for i in range(len(returned_customer_comment_count)):
    print(returned_customer_comment_count[i])
    if i == 10:
        break

# %% 加载 5d74981209839f61afb1c3bd14a8f891
user_hash = '5d74981209839f61afb1c3bd14a8f891'
print(user_hash)

# %% 加载 0f826a89cf68c399c5f4cf320c1a5842
user_hash = '0f826a89cf68c399c5f4cf320c1a5842'
print(user_hash)

# %% 加载 727999d580f3708378e3d903ddfa246d
user_hash = '727999d580f3708378e3d903ddfa246d'
print(user_hash)

# %% 获取用户的评论
user = returned_customer_comment[user_hash]
# 排序，按照字典里的 gmtModify 字段顺序排序
user = sorted(user, key=lambda x: x['gmtModify'])

with open('%s.csv' % user_hash, 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'commentId', 'userNickName', 'commentBody', 'commentGMTTime', 'commentTime', 'commentScore', 'hostReply',
        'productId', 'productName', 'productLatitude', 'productLongitude', 'productStress', 'productPrice',
        'productCommentCount', 'productStarRating', 'hostId', 'hostNickName', 'hostCommentCount',
        'hostAvgCommentScore', 'hostGoodCommentRate', 'hostProductCount'
    ])
    for comment in user:
        comment_id = comment['commentId']
        user_nickname = comment['userNickName']
        comment_body = comment['body']
        comment_gmt_time = comment['gmtModify']
        comment_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(comment_gmt_time / 1000))
        comment_score = comment['totalScore']
        host_reply = comment.get('hostReply')

        product_id = comment['rawProductId']
        product = db.get_product_detail_by_pid(product_id)
        product_info = product['product']['productAllInfoResult']
        product_name = product_info['title']
        add_info = product['product']['productAllInfoResult']['addressInfo']
        product_latitude = add_info['latitudeWGS']
        product_longitude = add_info['longitudeWGS']
        product_street = add_info['street']
        product_price = product['productPrice']['totalPrice']
        product_comment_count = product_info.get('commentNumber', 0) + product_info.get('extCommentNumber', 0)
        product_star_rating = product_info['starRating']

        host = product['hostInfo']
        host_id = host['userId']
        host_nick_name = host['nickName']
        host_comment_count = host['commentCount']
        host_avg_comment_score = host['avgCommentScore']
        host_good_comment_rate = host['goodCommentRate']
        host_product_count = host['productCount']

        writer.writerow([
            comment_id, user_nickname, comment_body, comment_gmt_time, comment_time, comment_score, host_reply,
            product_id, product_name, product_latitude, product_longitude, product_street, product_price,
            product_comment_count, product_star_rating, host_id, host_nick_name, host_comment_count,
            host_avg_comment_score, host_good_comment_rate, host_product_count
        ])

        # print('body: ', comment_body.replace('\n', '  '))
        # print('host_reply: ', host_reply)
        # print('comment_time: ', comment_time, 'product_id: ', product_id, 'host_id: ', host_id)
        # print('-' * 20)
