import redis
import numpy as np

r = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)


def get_user_activity(session):
    val = r.get(f"user_activity:{session}")
    return int(val) if val is not None else np.nan


def get_category_popularity(category_code):
    val = r.get(f"category_stats:{category_code}")
    return int(val) if val is not None else np.nan


def get_category_past_popularity(category_code, event_time):
    day = event_time.strftime("%Y%m%d")
    val = r.get(f"category:{category_code}:popularity:{day}")
    return int(val) if val is not None else 0


def get_user_past_activity_count(session_id, event_time):
    # IMPORTANT: do NOT use full timestamp in key
    val = r.get(f"user:{session_id}:activity_before")
    return int(val) if val is not None else 0
