import prcoords
import pymongo

MONGO_PATH = "mongodb://localhost:27017"


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
