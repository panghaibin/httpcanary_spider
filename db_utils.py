import pymongo
import logging

MONGO_PATH = "mongodb://localhost:27017"
logging.basicConfig(level=logging.INFO)


class DatabaseUtils:
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_PATH)
        self.db = self.client["mtms"]

    def save_user_info(self, data: list):
        if len(data) == 0:
            return
        col = self.db["user_info"]
        dc = 0
        ic = 0
        for user in data:
            d = col.delete_many({'userId': user['userId']})
            dc += d.deleted_count
            col.insert_one(user)
            ic += 1
        logging.info('user info, delete %s, insert %s', dc, ic)

    def save_host_product(self, data: list):
        if len(data) == 0:
            return
        col = self.db["host_product"]
        dc = 0
        ic = 0
        for product in data:
            d = col.delete_many({'productId': product['productId']})
            dc += d.deleted_count
            col.insert_one(product)
            ic += 1
        logging.info('host product, delete %s, insert %s', dc, ic)

    def save_product_detail(self, data: list):
        if len(data) == 0:
            return
        col = self.db["product_detail"]
        dc = 0
        ic = 0
        for detail in data:
            d = col.delete_many({'productId': detail['productId']})
            dc += d.deleted_count
            col.insert_one(detail)
            ic += 1
        logging.info('product detail, delete %s, insert %s', dc, ic)

    def save_user_comment(self, data: list):
        if len(data) == 0:
            return
        col = self.db["user_comment"]
        dc = 0
        ic = 0
        for comment in data:
            d = col.delete_many({'commentId': comment['commentId']})
            dc += d.deleted_count
            col.insert_one(comment)
            ic += 1
        logging.info('user comment, delete %s, insert %s', dc, ic)

    def save_product_comment(self, data: list):
        if len(data) == 0:
            return
        col = self.db["product_comment"]
        dc = 0
        ic = 0
        for comment in data:
            d = col.delete_many({'orderId': comment['orderId']})
            dc += d.deleted_count
            col.insert_one(comment)
            ic += 1
        logging.info('product comment, delete %s, insert %s', dc, ic)

    def save_product_ext_comment(self, data: list):
        if len(data) == 0:
            return
        col = self.db["product_ext_comment"]
        dc = 0
        ic = 0
        for comment in data:
            d = col.delete_many({'commentId': comment['commentId']})
            dc += d.deleted_count
            col.insert_one(comment)
            ic += 1
        logging.info('product ext comment, delete %s, insert %s', dc, ic)

    # def get_user_info_all(self):
    #     col = self.db["user_info"]
    #     return col.find()
    #
    # def get_user_info_by_uid(self, user_id):
    #     col = self.db["user_info"]
    #     return col.find_one({'userId': user_id})
    #
    # def get_host_product_all(self):
    #     col = self.db["host_product"]
    #     return col.find()
    #
    # def get_host_product_by_pid(self, product_id):
    #     col = self.db["host_product"]
    #     return col.find_one({'productId': product_id})
    #
    # def get_host_product_by_uid(self, user_id):
    #     col = self.db["host_product"]
    #     return col.find({'hostId': user_id})
    #
    # def get_user_comment_all(self):
    #     col = self.db["user_comment"]
    #     return col.find()
    #
    # def get_user_comment_by_uid(self, user_id):
    #     col = self.db["user_comment"]
    #     return col.find({'userId': user_id})
    #
    # def get_user_comment_by_pid(self, product_id):
    #     col = self.db["user_comment"]
    #     return col.find({'rawProductId': product_id})
    #
    # def get_user_comment_by_oid(self, order_id):
    #     col = self.db["user_comment"]
    #     return col.find({'orderId': order_id})

    def get_product_detail_all(self):
        col = self.db["product_detail"]
        return col.find({}, {'_id': 0})

    def get_product_detail_by_pid(self, product_id):
        col = self.db["product_detail"]
        return col.find_one({'productId': product_id}, {'_id': 0})

    def get_product_detail_by_uid(self, user_id):
        user_id = int(user_id)
        col = self.db["product_detail"]
        product_details = col.find({'hostInfo.userId': user_id}, {'_id': 0})
        return product_details

    def get_product_comment_all(self):
        col = self.db["product_comment"]
        return col.find({}, {'_id': 0})

    def get_product_comment_by_uid(self, user_id):
        user_id = int(user_id)
        product_ids = self.get_product_detail_by_uid(user_id)
        product_ids = [product['productId'] for product in product_ids]
        col = self.db["product_comment"]
        product_comments = col.find({'rawProductId': {'$in': product_ids}}, {'_id': 0})
        return product_comments

    def get_product_comment_by_pid(self, product_id):
        col = self.db["product_comment"]
        return col.find({'rawProductId': product_id}, {'_id': 0})

    def get_product_comment_by_oid(self, order_id):
        col = self.db["product_comment"]
        return dict(col.find_one({'orderId': order_id}, {'_id': 0}))

    def get_product_comment_by_cid(self, comment_id):
        col = self.db["product_comment"]
        return dict(col.find_one({'commentId': comment_id}, {'_id': 0}))

    def get_product_comment_by_score(self, value, symbol):
        col = self.db["product_comment"]
        symbol_map = {'<': '$lt', '>': '$gt', '=': '$eq', '<=': '$lte', '>=': '$gte'}
        return col.find({'totalScore': {symbol_map[symbol]: value}}, {'_id': 0})

    def get_product_comment_by_keyword(self, keyword):
        col = self.db["product_comment"]
        return col.find({'body': {'$regex': keyword}}, {'_id': 0})

    # def get_product_ext_comment_all(self):
    #     col = self.db["product_ext_comment"]
    #     return col.find({}, {'_id': 0})
    #
    # def get_product_ext_comment_by_uid(self, user_id):
    #     product_ids = self.get_product_detail_by_uid(user_id)
    #     product_ids = [product['productId'] for product in product_ids]
    #     col = self.db["product_ext_comment"]
    #     return col.find({'productId': {'$in': product_ids}}, {'_id': 0})
    #
    # def get_product_ext_comment_by_pid(self, product_id):
    #     col = self.db["product_ext_comment"]
    #     return col.find({'rawProductId': product_id}, {'_id': 0})
    #
    # def get_product_ext_comment_by_oid(self, order_id):
    #     col = self.db["product_ext_comment"]
    #     return col.find({'orderId': order_id}, {'_id': 0})
    #
    # def get_product_all_comment_all(self):
    #     comments = self.get_product_comment_all()
    #     ext_comments = self.get_product_ext_comment_all()
    #     return comments, ext_comments
    #
    # def get_product_all_comment_by_uid(self, user_id):
    #     comments = self.get_product_comment_by_uid(user_id)
    #     ext_comments = self.get_product_ext_comment_by_uid(user_id)
    #     return comments, ext_comments
    #
    # def get_product_all_comment_by_pid(self, product_id):
    #     comments = self.get_product_comment_by_pid(product_id)
    #     ext_comments = self.get_product_ext_comment_by_pid(product_id)
    #     return comments, ext_comments

    def get_host_info_all(self):
        raw_host_infos = self.get_product_detail_all()
        raw_host_infos = [host_info['hostInfo'] for host_info in raw_host_infos if raw_host_infos is not None]
        # 字典去重
        raw_host_infos = [dict(t) for t in set([tuple(d.items()) for d in raw_host_infos])]
        # 按照 userId 排序
        # raw_host_infos = sorted(raw_host_infos, key=lambda x: x['userId'])
        host_infos = []
        for raw_host_info in raw_host_infos:
            for i in range(len(host_infos)):
                if host_infos[i]['userId'] == raw_host_info['userId']:
                    host_infos.pop(i)
                    host_infos.insert(i, raw_host_info)
                    break
            else:
                host_infos.append(raw_host_info)
        return host_infos

    def get_host_info_by_uid(self, user_id):
        user_id = int(user_id)
        product_detail = self.get_product_detail_by_uid(user_id)
        if product_detail is None:
            return None
        product_detail = list(product_detail)
        if product_detail[0].get('hostInfo') is not None:
            return product_detail[0]['hostInfo']

    def get_host_info_by_pid(self, product_id):
        product_detail = self.get_product_detail_by_pid(product_id)
        if product_detail is None:
            return None
        if product_detail.get('hostInfo') is not None:
            return product_detail['hostInfo']

    def save_product_comment_sentiment(self, sentiment):
        col = self.db["product_comment_sentiment3"]
        comment_id = sentiment['commentId']
        page = sentiment['page']
        col.delete_many({'commentId': comment_id, 'page': page})
        col.insert_one(sentiment)

    def get_product_comment_sentiment_by_id(self, comment_id, page):
        col = self.db["product_comment_sentiment3"]
        return col.find_one({'commentId': comment_id, 'page': page}, {'_id': 0})

    def get_product_comment_avg_sentiment_by_id(self, comment_id):
        col = self.db["product_comment_sentiment3"]
        sentiments = col.find({'commentId': comment_id}, {'_id': 0})
        sentiments = list(sentiments)
        neg = 0
        pos = 0
        tol = 0
        body = ''
        for sentiment in sentiments:
            neg = neg + sentiment['Negative']
            pos = pos + sentiment['Positive']
            tol += 1
            body += sentiment['body']
        if tol == 0:
            return None
        avg_sentiment = {
            'commentId': comment_id,
            'Negative': neg / tol,
            'Positive': pos / tol,
            'body': body
        }
        return avg_sentiment

    def get_product_comment_sentiment_by_sentiment(self, type_, value, symbol):
        col = self.db["product_comment_sentiment3"]
        type_map = {'neg': 'Negative', 'neu': 'Neutral', 'pos': 'Positive'}
        symbol_map = {'<': '$lt', '>': '$gt', '=': '$eq', '<=': '$lte', '>=': '$gte'}
        return col.find({type_map[type_]: {symbol_map[symbol]: value}}, {'_id': 0})

    def check_product_comment_sentiment(self, comment_id):
        col = self.db["product_comment_sentiment3"]
        sentiment = col.find_one({'commentId': comment_id})
        if sentiment is None:
            return False
        else:
            return True

    def save_split_product_comment(self, data):
        col = self.db["split_product_comment"]
        comment_ids = [comment['commentId'] for comment in data]
        comment_ids = list(set(comment_ids))
        col.delete_many({'commentId': {'$in': comment_ids}})
        to_save = []
        for comment in data:
            for body in comment['body']:
                to_save.append({'commentId': comment['commentId'], 'body': body})
        col.insert_many(to_save)

    def get_split_product_comment_all(self):
        col = self.db["split_product_comment"]
        return list(col.find({}, {'_id': 0}))

    def get_split_product_comment_by_cid(self, comment_id):
        col = self.db["split_product_comment"]
        result = col.find_one({'commentId': comment_id}, {'_id': 0})
        return result

    def get_split_product_comment_by_keyword(self, keyword):
        col = self.db["split_product_comment"]
        result = list(col.find({'body': {'$regex': keyword}}, {'_id': 0}))
        return result

    def get_split_product_comment_by_multi_keywords(self, keywords: list):
        col = self.db["split_product_comment"]
        result = []
        for keyword in keywords:
            result += list(col.find({'body': {'$regex': keyword}}, {'_id': 0}))
        # remove duplicate dict
        # raw_host_infos = [dict(t) for t in set([tuple(d.items()) for d in raw_host_infos])]
        result = [dict(t) for t in set([tuple(d.items()) for d in result])]
        return result
