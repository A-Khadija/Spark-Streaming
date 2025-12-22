import joblib
import redis
import pandas as pd
from datetime import datetime
import time

# --- Configuration ---
MODEL_PATH = "/app/models/model.pkl"
REDIS_HOST = "redis"
REDIS_PORT = 6379
THRESHOLD = 0.4

# --- Initialization ---
# Load model assets once during startup
try:
    bundle = joblib.load(MODEL_PATH)
    model = bundle["model"]
    scaler = bundle["scaler"]
    features = bundle["features"]  # Exact list of model columns
except Exception as e:
    print(f"Error loading model at {MODEL_PATH}: {e}")
    raise

# Initialize Redis connection
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def predict_cart_event(event):
    """
    Perform real-time prediction for a single cart event.
    Expects 'event' as a dictionary from Kafka/Spark.
    """

    # 1. Fetch real-time features from Redis
    # Defaults to 0 if the keys do not exist
    user_activity = int(r.get(f"user_activity:{event['user_session']}") or 0)
    category_popularity = int(r.get(f"category_stats:{event['category_code']}") or 0)

    # 2. Prepare the feature dictionary (initialized with zeros)
    X_dict = {col: [0] for col in features}

    # 3. Fill numeric features
    X_dict["price"] = [event["price"]]
    X_dict["event_weekday"] = [event["event_weekday"]]
    X_dict["user_activity_count"] = [user_activity]
    X_dict["category_popularity"] = [category_popularity]

    # 4. Fill one-hot encoded categorical features
    if event.get("category_level1"):
        col1 = f"category_level1_{event['category_level1']}"
        if col1 in X_dict:
            X_dict[col1] = [1]

    if event.get("category_level2"):
        col2 = f"category_level2_{event['category_level2']}"
        if col2 in X_dict:
            X_dict[col2] = [1]

    # 5. Process data through the pipeline
    X = pd.DataFrame(X_dict)

    # Scale numeric columns
    numeric_cols = ["price", "user_activity_count", "category_popularity"]
    X[numeric_cols] = scaler.transform(X[numeric_cols])

    # Generate prediction and probability
    proba = model.predict_proba(X)[0][1]
    prediction = int(proba >= THRESHOLD)

    # 6. Store results in Redis for the dashboard/API
    # Update hash for session-specific state
    r.hset(
        f"prediction:{event['user_session']}",
        mapping={
            "probability": float(proba),
            "prediction": prediction,
            "timestamp": datetime.utcnow().isoformat(),
        },
    )

    # Add to a stream for time-series monitoring
    r.xadd(
        "stream:ml_metrics",
        {
            "ts": str(int(time.time() * 1000)),  # Timestamp in ms
            "probability": str(proba),
            "prediction": str(prediction),
            "price": str(event["price"]),
        },
        maxlen=10000,
    )

    return proba, prediction