import pymongo

MONGO_PATH = "mongodb://localhost:27017"


class DbSaver:
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_PATH)
        self.db = self.client["mtms"]

    def save_user_info(self, data):
        col = self.db["user_info"]
        col.delete_many({'user_id': data['userId']})
        col.insert_one(data)

    def save_host_products(self, data):
        col = self.db["host_product"]
        for product in data:
            col.delete_many({'productId': product['productId']})
        col.insert_many(data)

    def save_user_comments(self, data):
        col = self.db["user_comment"]
        for comment in data:
            col.delete_many({'commentId': comment['commentId']})
        col.insert_many(data)

    def save_product_comments(self, data):
        col = self.db["product_comment"]
        for comment in data:
            col.delete_many({'commentId': comment['commentId']})
        col.insert_many(data)
