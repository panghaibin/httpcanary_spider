from parse import *
from db_utils import *


class MTMSParser:
    def __init__(self, parse_dir_result: ParseDir):
        self._db = DbSaver()
        self._parse_dir_result = parse_dir_result
        self.user_info = self.get_user_info()
        self.host_product = self.get_host_product()
        self.product_detail = self.get_product_detail()
        self.user_comment = self.get_user_comment()
        self.product_comment = self.get_product_comment()

    def get_user_info(self):
        files = self._parse_dir_result.filter_req_path(r'user/info\?')
        user_info = []
        for file in files:
            user = file.response.json['data']
            user_info.append(user)
        return user_info

    def get_host_product(self):
        files = self._parse_dir_result.filter_req_path(r'searchProduct/listHostProduct\?')
        host_product = []
        for file in files:
            file_request = file.request.json
            host_id = file_request['hostId']
            products = file.response.json['data']['list']
            for product in products:
                product.update({'hostId': host_id})
            host_product.extend(products)
        return host_product

    def get_product_detail(self):
        files = self._parse_dir_result.filter_req_path(r'product/detail\?')
        product_detail = []
        for file in files:
            product_id = file.request.json['productId']
            data = file.response.json['data']
            data.update({'productId': product_id})
            product_detail.append(data)
        return product_detail

    def get_user_comment(self):
        files = self._parse_dir_result.filter_req_path(r'user/comments\?')
        user_comment = []
        for file in files:
            user_id = file.request.urlparse['userId'][0]
            comments = file.response.json['data']['list']
            for comment in comments:
                comment.update({'userId': user_id})
            user_comment.extend(comments)
        return user_comment

    def get_product_comment(self):
        files = self._parse_dir_result.filter_req_path(r'product/comments\?')
        product_comment = []
        for file in files:
            product_id = file.request.urlparse['productId'][0]
            data = file.response.json['data']
            comments = data['list']
            for comment in comments:
                comment.update({'rawProductId': product_id})
            product_comment.extend(comments)
        return product_comment

    def save_user_info(self):
        self._db.save_user_info(self.user_info)

    def save_host_product(self):
        self._db.save_host_product(self.host_product)

    def save_user_comment(self):
        self._db.save_user_comment(self.user_comment)

    def save_product_comment(self):
        self._db.save_product_comment(self.product_comment)

    def save_all(self):
        self.save_user_info()
        self.save_host_product()
        self.save_user_comment()
        self.save_product_comment()
