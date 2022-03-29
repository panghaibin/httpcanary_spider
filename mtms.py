from parse import *
from db_utils import *


class MTMSParser:
    def __init__(self, parse_dir_result: ParseDir):
        self._db = DbSaver()
        self._parse_dir_result = parse_dir_result
        self.user_info = self.get_user_info()
        self.host_products = self.get_host_products()
        self.user_comments = self.get_user_comments()
        self.product_comments = self.get_product_comments()

    def get_user_info(self):
        files = self._parse_dir_result.filter_req_path(r'user/info\?')
        user_info = {}
        for file in files:
            user_info.update(file.response.get_json()['data'])
        return user_info

    def get_host_products(self):
        files = self._parse_dir_result.filter_req_path(r'searchProduct/listHostProduct\?')
        host_products = []
        for file in files:
            file_request = file.request.get_json()
            host_id = file_request['hostId']
            products = file.response.get_json()['data']['list']
            for product in products:
                product.update({'hostId': host_id})
            host_products.extend(products)
        return host_products

    def get_user_comments(self):
        files = self._parse_dir_result.filter_req_path(r'user/comments\?')
        user_comments = []
        for file in files:
            user_id = file.request.get_urlparse()['userId'][0]
            comments = file.response.get_json()['data']['list']
            for comment in comments:
                comment.update({'userId': user_id})
            user_comments.extend(comments)
        return user_comments

    def get_product_comments(self):
        files = self._parse_dir_result.filter_req_path(r'product/comments\?')
        product_comments = []
        for file in files:
            product_id = file.request.get_urlparse()['productId'][0]
            data = file.response.get_json()['data']
            comments = data['list']
            for comment in comments:
                comment.update({'rawProductId': product_id})
            product_comments.extend(comments)
        return product_comments

    def save_user_info(self):
        self._db.save_user_info(self.user_info)

    def save_host_products(self):
        self._db.save_host_products(self.host_products)

    def save_user_comments(self):
        self._db.save_user_comments(self.user_comments)

    def save_product_comments(self):
        self._db.save_product_comments(self.product_comments)

    def save_all(self):
        self.save_user_info()
        self.save_host_products()
        self.save_user_comments()
        self.save_product_comments()
