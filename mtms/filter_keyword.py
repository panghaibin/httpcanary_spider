from mtms.data_clean import *
import csv

db = DatabaseUtils()
db_c = DataClean()
# db.save_split_product_comment(db_c.filter_split_product_comment())

# %%
neg_comments = db.get_product_comment_sentiment_by_sentiment('neg', 0.6, '>=')
neg_comments = list(neg_comments)
neg_comment_ids = [comment['commentId'] for comment in neg_comments]

low_score_comments = db.get_product_comment_by_score(30, '<=')
low_score_comments = list(low_score_comments)
low_score_comment_ids = [comment['commentId'] for comment in low_score_comments]

tol_comment_ids = neg_comment_ids + low_score_comment_ids
tol_comment_ids = list(set(tol_comment_ids))

# %%
keywords, des = ['位置', '定位', '地点', '选址', '地址', '方位'], 'position'
print(keywords, des)

# %%
keywords, des = ['空调', '制冷', '制热', '冷气', '暖气' '温控'], 'air-condition'
print(keywords, des)

# %%
keywords, des = ['房东', '房客', '房主', '主人', '老板'], 'boss'
print(keywords, des)

# %%
keywords, des = ['接', '送'], 'transfer'
print(keywords, des)

# %%
keyword_result = ''
result = db.get_split_product_comment_by_multi_keywords(keywords)
for i in result:
    if i['commentId'] in tol_comment_ids:
        keyword_result += i['body']
        # print(i['body'])

print(keyword_result)
with open('%s.txt' % des, 'w', encoding='utf-8') as f:
    f.write(keyword_result)

# %% 不符 关键词
filter_word, des = '不符', 'not-equal'
print(filter_word, des)

# %% 虚假 关键词
filter_word, des = '虚假', 'fake'
print(filter_word, des)

# %% 获取
result = db.get_split_product_comment_by_multi_keywords(keywords)
comment_ids = []
for i in result:
    if filter_word in i['body']:
        comment_ids.append(i['commentId'])

comment_ids = list(set(comment_ids))
# print(comment_ids)
with open('%s.csv' % des, 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'commentId', 'userNickName', 'commentBody', 'commentTime', 'commentScore', 'hostReply',
        'productId', 'productName', 'productLatitude', 'productLongitude', 'productStress', 'productPrice',
        'productCommentCount', 'productStarRating', 'hostId', 'hostNickName', 'hostCommentCount',
        'hostAvgCommentScore', 'hostGoodCommentRate', 'hostProductCount'
    ])
    for comment_id in comment_ids:
        comment = db.get_product_comment_by_cid(comment_id)
        product_id = comment['rawProductId']
        product = db.get_product_detail_by_pid(product_id)
        host = product['hostInfo']

        user_nick_name = comment['userNickName']
        comment_body = comment['body']
        comment_time = comment['gmtModify']
        comment_score = comment['totalScore']
        host_reply = comment.get('hostReply')

        product_info = product['product']['productAllInfoResult']
        product_name = product_info['title']
        # /data/product/productAllInfoResult/addressInfo/longitude
        add_info = product['product']['productAllInfoResult']['addressInfo']
        product_latitude = add_info['latitudeWGS']
        product_longitude = add_info['longitudeWGS']
        product_street = add_info['street']
        product_price = product['productPrice']['totalPrice']
        product_comment_count = product_info.get('commentNumber', 0) + product_info.get('extCommentNumber', 0)
        product_star_rating = product_info['starRating']

        host_id = host['userId']
        host_nick_name = host['nickName']
        host_comment_count = host['commentCount']
        host_avg_comment_score = host['avgCommentScore']
        host_good_comment_rate = host['goodCommentRate']
        host_product_count = host['productCount']

        writer.writerow([
            comment_id, user_nick_name, comment_body, comment_time, comment_score, host_reply,
            product_id, product_name, product_latitude, product_longitude, product_street, product_price,
            product_comment_count, product_star_rating, host_id, host_nick_name, host_comment_count,
            host_avg_comment_score, host_good_comment_rate, host_product_count
        ])
