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
        col = self.db["product_detail"]
        return col.find({'hostInfo': {'userId': user_id}}, {'_id': 0})

    def get_product_comment_all(self):
        col = self.db["product_comment"]
        return col.find({}, {'_id': 0})

    def get_product_comment_by_uid(self, user_id):
        product_ids = self.get_product_detail_by_uid(user_id)
        product_ids = [product['productId'] for product in product_ids]
        col = self.db["product_comment"]
        return col.find({'productId': {'$in': product_ids}}, {'_id': 0})

    def get_product_comment_by_pid(self, product_id):
        col = self.db["product_comment"]
        return col.find({'rawProductId': product_id}, {'_id': 0})

    def get_product_comment_by_oid(self, order_id):
        col = self.db["product_comment"]
        return col.find({'orderId': order_id}, {'_id': 0})

    def get_product_ext_comment_all(self):
        col = self.db["product_ext_comment"]
        return col.find({}, {'_id': 0})

    def get_product_ext_comment_by_uid(self, user_id):
        product_ids = self.get_product_detail_by_uid(user_id)
        product_ids = [product['productId'] for product in product_ids]
        col = self.db["product_ext_comment"]
        return col.find({'productId': {'$in': product_ids}}, {'_id': 0})

    def get_product_ext_comment_by_pid(self, product_id):
        col = self.db["product_ext_comment"]
        return col.find({'rawProductId': product_id}, {'_id': 0})

    def get_product_ext_comment_by_oid(self, order_id):
        col = self.db["product_ext_comment"]
        return col.find({'orderId': order_id}, {'_id': 0})

    def get_product_all_comment_all(self):
        comments = self.get_product_comment_all()
        ext_comments = self.get_product_ext_comment_all()
        return comments, ext_comments

    def get_product_all_comment_by_uid(self, user_id):
        comments = self.get_product_comment_by_uid(user_id)
        ext_comments = self.get_product_ext_comment_by_uid(user_id)
        return comments, ext_comments

    def get_product_all_comment_by_pid(self, product_id):
        comments = self.get_product_comment_by_pid(product_id)
        ext_comments = self.get_product_ext_comment_by_pid(product_id)
        return comments, ext_comments

    def get_host_info_all(self):
        host_infos = self.get_product_detail_all()
        host_infos = [host_info['hostInfo'] for host_info in host_infos if host_infos is not None]
        # 字典去重，userId 相同保留一个
        host_infos = [dict(t) for t in set([tuple(d.items()) for d in host_infos])]
        # 按照 userId 排序
        # host_infos = sorted(host_infos, key=lambda x: x['userId'])
        return host_infos

    def get_host_info_by_uid(self, user_id):
        product_detail = self.get_product_detail_by_uid(user_id)
        if product_detail is None:
            return None
        if product_detail[0].get('hostInfo') is not None:
            return product_detail[0]['hostInfo']

    def get_host_info_by_pid(self, product_id):
        product_detail = self.get_product_detail_by_pid(product_id)
        if product_detail is None:
            return None
        if product_detail.get('hostInfo') is not None:
            return product_detail['hostInfo']
