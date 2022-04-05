from db_utils import *


def remove_all_spaces(s):
    s = s.replace(' ', '')
    s = s.replace('\n', '')
    s = s.replace('\t', '')
    s = s.replace('\r', '')
    return s


db = DatabaseUtils()

# %% 获取负面评分>=0.6
neg_comments = db.get_product_comment_sentiment_by_sentiment('neg', 0.6, '>=')
neg_comments = list(neg_comments)

# %% 获取评论三星(30)及以下
low_score_comments = db.get_product_comment_by_score(30, '<=')
low_score_comments = list(low_score_comments)

# %% 去重
for neg_comment in neg_comments:
    for i, low_score_comment in enumerate(low_score_comments):
        neg_comment_id = neg_comment['commentId']
        low_score_comment_id = low_score_comment['commentId']
        low_score_comment_body = low_score_comment.get('body')
        if neg_comment_id == low_score_comment_id or low_score_comment_body is None \
                or len(remove_all_spaces(low_score_comment_body)) == 0:
            low_score_comments.pop(i)

comments = low_score_comments + neg_comments
print(comments)
