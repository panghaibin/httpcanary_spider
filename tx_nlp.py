import json
import re
import logging
from db_utils import DatabaseUtils
from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.nlp.v20190408 import nlp_client, models


def delete_emoji_word(text):
    """
    去除表情符号
    :param text:
    :return:
    """
    return re.sub(r'[\U00010000-\U0010ffff]', '', text)


class TXNLP:
    def __init__(self):
        self.id_, self.key = self.get_id_key()

    @staticmethod
    def get_id_key():
        with open('tx_nlp.json', 'r') as f:
            id_key = json.load(f)
        return id_key['id'], id_key['key']

    def get_lexical_analysis_result(self, text):
        cred = credential.Credential(self.id_, self.key)
        httpProfile = HttpProfile()
        httpProfile.endpoint = "nlp.tencentcloudapi.com"

        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        client = nlp_client.NlpClient(cred, "ap-guangzhou", clientProfile)

        req = models.LexicalAnalysisRequest()
        params = {
            "Text": text,
        }
        req.from_json_string(json.dumps(params))

        resp = client.LexicalAnalysis(req)
        return json.loads(resp.to_json_string())

    def get_sentiment_analysis(self, text):
        cred = credential.Credential(self.id_, self.key)
        httpProfile = HttpProfile()
        httpProfile.endpoint = "nlp.tencentcloudapi.com"

        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        client = nlp_client.NlpClient(cred, "ap-guangzhou", clientProfile)

        req = models.SentimentAnalysisRequest()
        params = {
            "Text": text,
            "Flag": 3,
        }
        req.from_json_string(json.dumps(params))

        resp = client.SentimentAnalysis(req)
        return json.loads(resp.to_json_string())


if __name__ == '__main__':
    txnlp = TXNLP()
    db = DatabaseUtils()
    comments = list(db.get_product_comment_all())
    for comment in comments:
        comment_id = comment['commentId']
        # if True:
        if not db.check_product_comment_sentiment(comment_id):
            body = comment.get('body')
            if not body:
                continue

            body = delete_emoji_word(body)
            body = body.replace('\n', '')
            body = body.replace('\r', '')
            body = body.replace(' ', '')

            text_list = [body[i:i+200] for i in range(0, len(body), 200)]
            # txnlp_result = db.get_product_comment_sentiment(comment_id, 1)
            # if txnlp_result is None:
            #     continue
            # txnlp_result.update({'page': 1, 'totalPage': len(text_list)})
            # db.save_product_comment_sentiment(txnlp_result)
            # if len(body) <= 200:
            #     continue

            for i in range(0, len(text_list)):
                try:
                    txnlp_result = txnlp.get_sentiment_analysis(text_list[i])
                except Exception as err:
                    logging.error(err)
                    logging.error('get sentiment analysis error, "commentId": %s, page: %s', comment_id, i)
                    logging.error(text_list[i])
                    continue
                if txnlp_result:
                    txnlp_result.update({
                        'commentId': comment_id,
                        'body': text_list[i],
                        'page': i + 1,
                        'totalPage': len(text_list)
                    })
                    db.save_product_comment_sentiment(txnlp_result)
                    logging.debug('commentId: %s, sentiment: %s, page: %s', comment_id, txnlp_result['Sentiment'], i)
                else:
                    logging.error('commentId: %s, sentiment: %s', comment_id, 'fail')
        else:
            logging.debug('commentId: %s, sentiment exits', comment_id)
