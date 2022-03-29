import pymongo

MONGO_PATH = "mongodb://localhost:27017"


class DbSaver:
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_PATH)
        self.db = self.client["mtms"]

    def save_user_info(self, data: list):
        col = self.db["user_info"]
        for user in data:
            col.delete_many({'userId': user['userId']})
        col.insert_many(data)

    def save_host_product(self, data: list):
        col = self.db["host_product"]
        for product in data:
            col.delete_many({'productId': product['productId']})
        col.insert_many(data)

    def save_user_comment(self, data: list):
        col = self.db["user_comment"]
        for comment in data:
            col.delete_many({'commentId': comment['commentId']})
        col.insert_many(data)

    def save_product_comment(self, data: list):
        col = self.db["product_comment"]
        for comment in data:
            col.delete_many({'commentId': comment['commentId']})
        col.insert_many(data)
