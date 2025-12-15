import redis
import numpy as np

r = redis.Redis(host="redis", port=6379, db=0)


def get_user_activity(session):
    val = r.get(f"user_activity:{session}")
    return int(val) if val is not None else np.nan


def get_category_popularity(category_code):
    val = r.get(f"category_stats:{category_code}")
    return int(val) if val is not None else np.nan
