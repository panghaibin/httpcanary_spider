import requests
from time import sleep
from parse import *
from db_utils import *
from urllib.parse import urlencode


class MTMSParser:
    def __init__(self, parse_dir_result: ParseDir):
        self._db = DbSaver()
        self._parse_dir_result = parse_dir_result
        self.user_info = self.get_user_info()
        self.host_product = self.get_host_product()
        self.product_detail = self.get_product_detail()
        self.user_comment = self.get_user_comment()
        self.product_comment = self.get_product_comment()
        self.product_ext_comment = self.get_product_ext_comment()

    def get_user_info(self):
        files = self._parse_dir_result.filter_req_path(r'user/info\/')
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
                product.update({'hostId': int(host_id)})
            host_product.extend(products)
        return host_product

    def get_product_detail(self):
        files = self._parse_dir_result.filter_req_path(r'product/detail\?')
        product_detail = []
        for file in files:
            product_id = file.request.json['productId']
            data = file.response.json['data']
            data.update({'productId': int(product_id)})
            logging.info('pid: %s', product_id)
            product_detail.append(data)
        return product_detail

    def get_user_comment(self):
        files = self._parse_dir_result.filter_req_path(r'user/comments\?')
        user_comment = []
        for file in files:
            user_id = file.request.urlparse_query['userId']
            comments = file.response.json['data']['list']
            for comment in comments:
                comment.update({'userId': int(user_id)})
            user_comment.extend(comments)
        return user_comment

    def get_product_comment(self):
        files = self._parse_dir_result.filter_req_path(r'product/comments\?')
        product_comment = []
        for file in files:
            product_id = file.request.urlparse_query['productId']
            data = file.response.json['data']
            comments = data['list']
            for comment in comments:
                comment.update({'rawProductId': int(product_id)})
            product_comment.extend(comments)
        return product_comment

    def get_product_ext_comment(self):
        files = self._parse_dir_result.filter_req_path(r'product/extCommentList\?')
        product_ext_comment = []
        for file in files:
            product_id = file.request.json['productId']
            data = file.response.json['data']
            comments = data['list']
            for comment in comments:
                comment.update({'rawProductId': int(product_id)})
            product_ext_comment.extend(comments)
        return product_ext_comment

    def save_user_info(self):
        self._db.save_user_info(self.user_info)

    def save_host_product(self):
        self._db.save_host_product(self.host_product)

    def save_product_detail(self):
        self._db.save_product_detail(self.product_detail)

    def save_user_comment(self):
        self._db.save_user_comment(self.user_comment)

    def save_product_comment(self):
        self._db.save_product_comment(self.product_comment)

    def save_product_ext_comment(self):
        self._db.save_product_ext_comment(self.product_ext_comment)

    def save_all(self):
        self.save_user_info()
        self.save_host_product()
        self.save_product_detail()
        self.save_user_comment()
        self.save_product_comment()
        self.save_product_ext_comment()


class MTMSComment:
    def __init__(self, parse_dir_result: ParseDir):
        self._mtms_parser = parse_dir_result
        self._db = DbSaver()
        self.product_comment = self._mtms_parser.filter_req_path(r'product/comments\?')
        self.need_more_product_comment = {}
        self.check_more_product_comment()
        self.more_product_comment_result = []
        self.product_ext_comment = self._mtms_parser.filter_req_path(r'product/extCommentList\?')
        self.need_more_product_ext_comment = {}
        self.check_more_product_ext_comment()
        self.more_product_ext_comment_result = []

    def check_more_product_comment(self):
        for comment in self.product_comment:
            if comment.response.json['data']['total'] > 0:
                host = comment.request.request.headers.get('host')
                path_no_query = comment.request.get_no_query_path()
                req_query = comment.request.urlparse_query
                product_id = int(req_query['productId'])
                page_now = int(req_query['pageNow'])
                page_size = int(req_query['pageSize'])
                res_json = comment.response.json
                tol_comment = res_json['data']['total']
                tol_page = int(tol_comment / page_size) + 1
                if tol_page > 1 and page_now == 1 and self.need_more_product_comment.get(product_id) is None:
                    send_list = []
                    for page in range(2, tol_page + 1):
                        req_query.update({'pageNow': page})
                        url = "https://%s%s?%s" % (host, path_no_query, urlencode(req_query))
                        headers = dict(comment.request.request.headers)
                        body = comment.request.json
                        send = {'url': url, 'headers': headers, 'body': body}
                        send_list.append(send)
                    self.need_more_product_comment.update({product_id: send_list})
        logging.info('need_more_product_comment: %s' % len(self.need_more_product_comment))

    def get_more_product_comment(self):
        for product_id in self.need_more_product_comment:
            for send in self.need_more_product_comment[product_id]:
                url = send['url']
                headers = send['headers']
                body = send['body']
                result = requests.post(url, headers=headers, data=body).json()['data']['list']
                for comment in result:
                    comment.update({'rawProductId': product_id})
                    # print(comment)
                self.more_product_comment_result.extend(result)
                sleep(1)
            logging.info('get_more_product_comment: id%s' % product_id)

    def save_more_product_comment(self):
        data = self.more_product_comment_result
        if len(data) == 0:
            return
        self._db.save_product_comment(data)
        logging.info('save_more_product_comment: %s' % len(data))
        
    def check_more_product_ext_comment(self):
        for comment in self.product_ext_comment:
            if comment.response.json['data']['total'] > 0:
                host = comment.request.request.headers.get('host')
                path_no_query = comment.request.get_no_query_path()
                req_query = comment.request.urlparse_query
                req_json = comment.request.json
                product_id = int(req_json['productId'])
                page_now = int(req_json['pageNow'])
                page_size = int(req_json['pageSize'])
                res_json = comment.response.json
                tol_comment = res_json['data']['total']
                tol_page = int(tol_comment / page_size) + 1
                if tol_page > 1 and page_now == 1 and self.need_more_product_ext_comment.get(product_id) is None:
                    send_list = []
                    for page in range(2, tol_page + 1):
                        req_query.update({'pageNow': page})
                        url = "https://%s%s?%s" % (host, path_no_query, urlencode(req_query))
                        headers = dict(comment.request.request.headers)
                        body = comment.request.json
                        send = {'url': url, 'headers': headers, 'body': body}
                        send_list.append(send)
                    self.need_more_product_ext_comment.update({product_id: send_list})
        logging.info('need_more_ext_product_comment: %s' % len(self.need_more_product_ext_comment))

    def get_more_product_ext_comment(self):
        for product_id in self.need_more_product_ext_comment:
            for send in self.need_more_product_ext_comment[product_id]:
                url = send['url']
                headers = send['headers']
                body = send['body']
                result = requests.post(url, headers=headers, data=body).json()['data']['list']
                for comment in result:
                    comment.update({'rawProductId': product_id})
                    # print(comment)
                self.more_product_ext_comment_result.extend(result)
                sleep(1)
            logging.info('get_more_product_ext_comment: id%s' % product_id)

    def save_more_product_ext_comment(self):
        data = self.more_product_ext_comment_result
        if len(data) == 0:
            return
        self._db.save_product_ext_comment(data)
        logging.info('save_more_product_ext_comment: %s' % len(data))

    def get_all(self):
        self.get_more_product_comment()
        # self.get_more_product_ext_comment()

    def save_all(self):
        self.save_more_product_comment()
        # self.save_more_product_ext_comment()
