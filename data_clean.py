import hashlib
import os

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
            ext_comment.update({'commentDate': ext_comment['extCommentDate']})
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
                if comments[i].get('userAvatarUrl') == comments[j].get('userAvatarUrl') \
                        and comments[i].get('userNickName') == comments[j].get('userNickName') \
                        and '匿名用户' not in comments[i].get('userNickName'):
                    key = comments[i].get('userAvatarUrl') + comments[i].get('userNickName')
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
