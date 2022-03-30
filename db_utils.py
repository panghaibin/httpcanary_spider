import pymongo
import logging

MONGO_PATH = "mongodb://localhost:27017"
logging.basicConfig(level=logging.INFO)


class DbSaver:
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
