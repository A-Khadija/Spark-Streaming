import joblib
import redis
import pandas as pd
from datetime import datetime
import time

MODEL_PATH = "/app/models/model.pkl"

REDIS_HOST = "redis"
REDIS_PORT = 6379

# Load model once
bundle = joblib.load(MODEL_PATH)
model = bundle["model"]
scaler = bundle["scaler"]
features = bundle["features"]  # liste exacte des colonnes du modèle

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

THRESHOLD = 0.4


def predict_cart_event(event):
    """
    Perform real-time prediction for a single cart event
    event: dict from Kafka/Spark
    """

    # ---- Get real-time features from Redis ----
    user_activity = int(r.get(f"user_activity:{event['user_session']}") or 0)
    category_popularity = int(r.get(f"category_stats:{event['category_code']}") or 0)

    # ---- Initialize feature dict with zeros ----
    X_dict = {col: [0] for col in features}

    # ---- Fill numeric features ----
    X_dict["price"] = [event["price"]]
    X_dict["event_weekday"] = [event["event_weekday"]]
    X_dict["user_activity_count"] = [user_activity]
    X_dict["category_popularity"] = [category_popularity]

    # ---- Fill one-hot features for category_level1/2 ----
    if event.get("category_level1"):
        col1 = f"category_level1_{event['category_level1']}"
        if col1 in X_dict:
            X_dict[col1] = [1]

    if event.get("category_level2"):
        col2 = f"category_level2_{event['category_level2']}"
        if col2 in X_dict:
            X_dict[col2] = [1]

    # ---- Convert to DataFrame ----
    X = pd.DataFrame(X_dict)

    # ---- Scale numeric columns ----
    numeric_cols = ["price", "user_activity_count", "category_popularity"]
    X[numeric_cols] = scaler.transform(X[numeric_cols])

    # ---- Make prediction ----
    proba = model.predict_proba(X)[0][1]
    prediction = int(proba >= THRESHOLD)

    # ---- Store prediction in Redis ----
    r.hset(
        f"prediction:{event['user_session']}",
        mapping={
            "probability": float(proba),
            "prediction": prediction,
            "timestamp": datetime.utcnow().isoformat(),
        },
    )

    r.xadd(
        "stream:ml_metrics",
        {
            "ts": str(int(time.time() * 1000)),  # 👈 timestamp in ms
            "probability": str(proba),
            "prediction": str(prediction),
            "price": str(event["price"]),
        },
        maxlen=10000,
    )

    return proba, prediction
