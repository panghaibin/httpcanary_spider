import pymongo
import logging

MONGO_PATH = "mongodb://localhost:27017"
logging.basicConfig(level=logging.INFO)


class DbSaver:
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_PATH)
        self.db = self.client["mtms"]

    def save_user_info(self, data: list):
        col = self.db["user_info"]
        dc = 0
        for user in data:
            d = col.delete_many({'userId': user['userId']})
            dc += d.deleted_count
        logging.info('delete user info: %s', dc)
        i = col.insert_many(data)
        logging.info('insert user info: %s', len(i.inserted_ids))

    def save_host_product(self, data: list):
        col = self.db["host_product"]
        dc = 0
        for product in data:
            d = col.delete_many({'productId': product['productId']})
            dc += d.deleted_count
        logging.info('delete host product: %s', dc)
        i = col.insert_many(data)
        logging.info('insert host product: %s', len(i.inserted_ids))

    def save_product_detail(self, data: list):
        col = self.db["product_detail"]
        dc = 0
        for detail in data:
            d = col.delete_many({'productId': detail['productId']})
            dc += d.deleted_count
        logging.info('delete product detail: %s', dc)
        i = col.insert_many(data)
        logging.info('insert product detail: %s', len(i.inserted_ids))

    def save_user_comment(self, data: list):
        col = self.db["user_comment"]
        dc = 0
        for comment in data:
            d = col.delete_many({'commentId': comment['commentId']})
            dc += d.deleted_count
        logging.info('delete user comment: %s', dc)
        i = col.insert_many(data)
        logging.info('insert user comment: %s', len(i.inserted_ids))

    def save_product_comment(self, data: list):
        col = self.db["product_comment"]
        dc = 0
        for comment in data:
            d = col.delete_many({'commentId': comment['commentId']})
            dc += d.deleted_count
        logging.info('delete product comment: %s', dc)
        i = col.insert_many(data)
        logging.info('insert product comment: %s', len(i.inserted_ids))
