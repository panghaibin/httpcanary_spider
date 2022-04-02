import csv
import hashlib
import os
import time
import prcoords
import logging
import pymongo
import json
import re

from db_utils import DatabaseUtils

MONGO_PATH = "mongodb://localhost:27017"
logging.basicConfig(level=logging.INFO)


class DataClean:
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_PATH)
        self.db = self.client["mtms"]
        self.db_utils = DatabaseUtils()
        self.returned_customer_comment = None
        self.same_product_customer_comment = None
        self.same_host_customer_comment = None
        self.returned_customer_of_host = None

    def transform_coord_product_detail(self):
        col = self.db["product_detail"]
        product_details = col.find()
        for product in product_details:
            add_info = product['product']['productAllInfoResult']['addressInfo']
            if not (add_info.get('latitudeWGS') and add_info.get('longitudeWGS')):
                lat = add_info['latitude'] / 1000000
                lon = add_info['longitude'] / 1000000
                wgs = prcoords.gcj_wgs((lat, lon))
                add_info.update({'latitudeWGS': wgs.lat})
                add_info.update({'longitudeWGS': wgs.lon})
                col.update_one({'productId': product['productId']},
                               {'$set': {'product.productAllInfoResult.addressInfo': add_info}})

    @staticmethod
    def parse_comment2tag_text(body):
        tags = re.findall(r'#(.*?)#', body)
        tags = list(set(tags))
        text = str.strip(re.sub(r'#.*?#', '', body))
        return tags, text

    def transform_ext_comment(self):
        col = self.db["product_comment"]
        ext_col = self.db["product_ext_comment"]
        ext_comments = ext_col.find()
        dc = 0
        ic = 0
        for ext_comment in ext_comments:
            ext_comment.pop('_id')
            body = ext_comment['body']
            tags, text = self.parse_comment2tag_text(body)
            ext_comment.update({'body': text})
            ext_comment.update({'commentTextList': tags})
            # 格式： 2021年06月
            comment_date = ext_comment['extCommentDate']
            ext_comment.update({'commentDate': comment_date})
            # 转换成时间戳
            gmt_time = time.strptime(comment_date, '%Y年%m月')
            ext_comment.update({'gmtModify': int(time.mktime(gmt_time) * 1000)})
            ext_comment.pop('extCommentDate')
            ext_comment.pop('totalScoreDesc')
            d = col.delete_many({'commentId': ext_comment['commentId'], 'source': 2})
            dc += d.deleted_count
            col.insert_one(ext_comment)
            ic += 1
        logging.info("delete comment id same and source is 2 in product_comment: %d" % dc)
        logging.info("insert ext comment to product_comment: %d" % ic)

    def join_product_detail_comment(self):
        product_col = self.db["product_detail"]
        comment_col = self.db["product_comment"]
        product_details = product_col.find({}, {'_id': 0})
        output_product_details = []
        for product in product_details:
            product.pop('availableHint')
            product.pop('benefit')
            product.pop('depositSwitch')
            product.pop('guaranteeModelList')
            product.pop('guestInfo')
            product.pop('limitHintResult')
            product_id = product['productId']
            comments = comment_col.find({'rawProductId': product_id}, {'_id': 0})
            output_comments = []
            for comment in comments:
                comment.pop('rawProductId')
                comment.pop('commentId')
                comment.pop('guestId') if comment.get('guestId') else None
                comment.pop('hostId')
                comment.pop('productId') if comment.get('productId') else None
                comment.pop('orderId') if comment.get('orderId') else None
                output_comments.append(comment)
            product.update({'comments': output_comments})
            output_product_details.append(product)
        return output_product_details

    def export_json_product_detail_comment(self, file_name=''):
        product_detail_comment = self.join_product_detail_comment()
        file_name = file_name if len(file_name) > 0 else 'product_detail_comment.json'
        file_name = file_name + '.json' if not file_name.endswith('.json') else file_name
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(product_detail_comment, f, ensure_ascii=False, indent=2)
        logging.info("export product detail and comment to %s, file size: %s", file_name, os.path.getsize(file_name))

    def filter_returned_customer_comment(self):
        col = self.db["product_comment"]
        comments = list(col.find({}, {'_id': 0}))
        same = {}
        for i in range(len(comments)):
            for j in range(i + 1, len(comments)):
                i_name = re.sub(r'\*', '', comments[i].get('userNickName'))
                j_name = re.sub(r'\*', '', comments[j].get('userNickName'))
                if comments[i].get('userAvatarUrl') == comments[j].get('userAvatarUrl') \
                        and i_name == j_name \
                        and '匿名用户' not in comments[i].get('userNickName'):
                    key = comments[i].get('userAvatarUrl') + i_name
                    hash_key = hashlib.md5(key.encode('utf-8')).hexdigest()
                    if hash_key not in same:
                        same.update({hash_key: [comments[i], comments[j]]})
                    else:
                        for exit_comment in same.get(hash_key):
                            if exit_comment.get('commentId') == comments[j].get('commentId'):
                                break
                        else:
                            same.get(hash_key).append(comments[j])
        self.returned_customer_comment = same
        return same

    def filter_same_product_customer_comment(self):
        if self.returned_customer_comment is None:
            self.filter_returned_customer_comment()
        same = self.returned_customer_comment
        same2 = {}
        for i in same:
            comments = same[i]
            for j in range(len(comments)):
                for k in range(j + 1, len(comments)):
                    if comments[j]['rawProductId'] == comments[k]['rawProductId']:
                        if i not in same2:
                            same2.update({i: [comments[j], comments[k]]})
                        else:
                            for exit_comment in same2.get(i):
                                if exit_comment['commentId'] == comments[k]['commentId']:
                                    break
                            else:
                                same2.get(i).append(comments[k])
        self.same_product_customer_comment = same2
        return same2

    def filter_same_host_customer_comment(self):
        if self.returned_customer_comment is None:
            self.filter_returned_customer_comment()
        same = self.returned_customer_comment
        same2 = {}
        for i in same:
            comments = same[i]
            for j in range(len(comments)):
                for k in range(j + 1, len(comments)):
                    j_host_id = self.db_utils.get_host_info_by_pid(comments[j]['rawProductId'])
                    k_host_id = self.db_utils.get_host_info_by_pid(comments[k]['rawProductId'])
                    if j_host_id is not None and j_host_id['userId'] == k_host_id['userId']:
                        if i not in same2:
                            same2.update({i: [comments[j], comments[k]]})
                        else:
                            for exit_comment in same2.get(i):
                                if exit_comment['commentId'] == comments[k]['commentId']:
                                    break
                            else:
                                same2.get(i).append(comments[k])
        self.same_host_customer_comment = same2
        return same2

    def filter_returned_customer_comment_of_host(self):
        hosts = self.db_utils.get_host_info_all()
        same0 = {}
        for host in hosts:
            same = {}
            host_id = host['userId']
            comments = list(self.db_utils.get_product_comment_by_uid(host_id))
            for i in range(len(comments)):
                for j in range(i + 1, len(comments)):
                    i_name = re.sub(r'\*', '', comments[i].get('userNickName'))
                    j_name = re.sub(r'\*', '', comments[j].get('userNickName'))
                    if comments[i].get('userAvatarUrl') == comments[j].get('userAvatarUrl') \
                            and i_name == j_name \
                            and '匿名用户' not in comments[i].get('userNickName'):
                        key = comments[i].get('userAvatarUrl') + i_name
                        hash_key = hashlib.md5(key.encode('utf-8')).hexdigest()
                        if hash_key not in same:
                            same.update({hash_key: [comments[i], comments[j]]})
                        else:
                            for exit_comment in same.get(hash_key):
                                if exit_comment.get('commentId') == comments[j].get('commentId'):
                                    break
                            else:
                                same.get(hash_key).append(comments[j])
            if len(same) > 0:
                same0.update({host_id: same})
        self.returned_customer_of_host = same0
        return same0

    def save_returned_customer_comment_of_host2csv(self, file_name=None):
        if self.returned_customer_of_host is None:
            self.filter_returned_customer_comment_of_host()
        returned_customer_comment_of_host = self.returned_customer_of_host
        file_name = file_name if file_name is not None else 'returned_customer_comment_of_host.csv'
        file_name = file_name + '.csv' if file_name[-4:] != '.csv' else file_name
        with open(file_name, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['userId', 'nickName', 'goodCommentRate', 'goodCommentRateAbove', 'replySessionRate',
                             'avgCommentScore', 'commentCount', 'productCount', 'returnedCustomerCount',
                             'returnedCommentCount', 'earliestCommentDate'])
            for host_id in returned_customer_comment_of_host:
                host_id = host_id
                host = self.db_utils.get_host_info_by_uid(int(host_id))
                nick_name = host['nickName']
                good_comment_rate = host['goodCommentRate']
                good_comment_rate_above = host['goodCommentRateAbove']
                reply_session_rate = host['replySessionRate']
                avg_comment_score = host['avgCommentScore']
                comment_count = host['commentCount']
                product_count = host['productCount']
                returned_customer_count = 0
                returned_comment_count = 0
                comment_date_list = []
                for user in returned_customer_comment_of_host[host_id]:
                    returned_customer_count += 1
                    for comment in returned_customer_comment_of_host[host_id][user]:
                        returned_comment_count += 1
                        comment_date_list.append(comment['gmtModify'])
                earliest_comment_date = min(comment_date_list)
                writer.writerow([host_id, nick_name, good_comment_rate, good_comment_rate_above, reply_session_rate,
                                 avg_comment_score, comment_count, product_count, returned_customer_count,
                                 returned_comment_count, earliest_comment_date])
        logging.info('save returned customer comment of host to csv file: %s, file size: %s',
                     file_name, os.path.getsize(file_name))
