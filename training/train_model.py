import pandas as pd
import psycopg2
import joblib
import os
import numpy as np
from sklearn.model_selection import train_test_split
from lightgbm import LGBMClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, roc_auc_score

from redis_utils import (
    get_user_past_activity_count,
    get_category_past_popularity
)

# ================= CONFIG =================
POSTGRES_CONFIG = {
    "host": "postgres",
    "database": "ecommerce",
    "user": "user",
    "password": "password",
}

MODEL_PATH = "/app/models/model.pkl"

FEATURE_COLUMNS = [
    "price",
    "event_weekday",
    "category_level1",
    "category_level2",
    "user_activity_count",
    "category_popularity",
]

# ================= LOAD DATA =================
print("📥 Loading data from PostgreSQL...")

conn = psycopg2.connect(**POSTGRES_CONFIG)

query = """
SELECT
    user_session,
    event_time,
    event_type,
    price,
    event_weekday,
    category_level1,
    category_level2,
    category_code
FROM raw_events
WHERE event_type IN ('cart', 'purchase')
ORDER BY user_session, event_time
"""

df = pd.read_sql(query, conn)
conn.close()


print(f" Loaded {len(df)} rows")

# ================= FEATURE ENGINEERING =================
purchase_sessions = set(
    df[df["event_type"] == "purchase"]["user_session"]
)
cart_df = df[df["event_type"] == "cart"].copy()
cart_df["label"] = cart_df["user_session"].isin(purchase_sessions).astype(int)

cart_df["user_activity_count"] = cart_df.apply(
    lambda r: get_user_past_activity_count(
        r["user_session"], r["event_time"]
    ),
    axis=1
)

cart_df["category_popularity"] = cart_df.apply(
    lambda r: get_category_past_popularity(
        r["category_code"], r["event_time"]
    ),
    axis=1
)

# Safe fallback
cart_df["user_activity_count"].fillna(0, inplace=True)
cart_df["category_popularity"].fillna(0, inplace=True)

cart_df["hour"] = cart_df["event_time"].dt.hour
cart_df["is_weekend"] = cart_df["event_weekday"].isin([5, 6]).astype(int)


cart_df["price_vs_category_avg"] = (
    cart_df["price"] /
    cart_df.groupby("category_code")["price"].transform("mean")
)


# ================= ONE-HOT CATEGORICAL =================
categorical_cols = ["category_level1", "category_level2"]
cart_df = pd.get_dummies(cart_df, columns=categorical_cols, drop_first=True)

# ================= SCALE NUMERIC FEATURES =================
numeric_cols = [
    "price",
    "user_activity_count",
    "category_popularity"
]

scaler = StandardScaler()
cart_df[numeric_cols] = scaler.fit_transform(cart_df[numeric_cols])


# ================= FEATURES / TARGET =================
X = cart_df.drop(
    columns=[
        "event_type",
        "user_session",
        "event_time",
        "category_code",
        "label"
    ]
)

y = cart_df["label"]

# ================= TRAIN / TEST SPLIT =================
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y
)


# ================= MODEL =================
print(" Training XGBOOST Model..")


model = LGBMClassifier(
    n_estimators=500,
    learning_rate=0.05,
    max_depth=-1,
    num_leaves=31,
    subsample=0.8,
    colsample_bytree=0.8,
    class_weight="balanced",
    random_state=42,
)




# ================= EVALUATION =================
model.fit(
    X_train,
    y_train # keep this if supported
)



y_proba = model.predict_proba(X_test)[:, 1]
THRESHOLD = 0.4
y_pred = (y_proba >= THRESHOLD).astype(int)

print(classification_report(y_test, y_pred))
print("ROC AUC:", roc_auc_score(y_test, y_proba))

# ================= SAVE MODEL =================
os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
joblib.dump(
    {"model": model, "scaler": scaler, "features": X.columns.tolist()}, MODEL_PATH
)
print(f" Model saved to {MODEL_PATH}")
