import prcoords
import logging
import pymongo
import re

MONGO_PATH = "mongodb://localhost:27017"
logging.basicConfig(level=logging.INFO)


class DataClean:
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_PATH)
        self.db = self.client["mtms"]

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
