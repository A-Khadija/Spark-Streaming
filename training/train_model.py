import pandas as pd
import psycopg2
import joblib
import os
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, roc_auc_score

from redis_utils import get_user_activity, get_category_popularity

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
    price,
    event_weekday,
    category_level1,
    category_level2,
    event_type,
    user_session,
    category_code
FROM raw_events
WHERE event_type IS NOT NULL
"""
df = pd.read_sql(query, conn)
conn.close()

print(f"✅ Loaded {len(df)} rows")

# ================= LOAD REDIS FEATURES =================
df["user_activity_count"] = df["user_session"].apply(get_user_activity)
df["user_activity_count"].fillna(df["user_activity_count"].median(), inplace=True)

df["category_popularity"] = df["category_code"].apply(get_category_popularity)
df["category_popularity"].fillna(df["category_popularity"].median(), inplace=True)

# ================= LABEL =================
df["label"] = (df["event_type"] == "purchase").astype(int)

# ================= ONE-HOT CATEGORICAL =================
categorical_cols = ["category_level1", "category_level2"]
df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

# ================= SCALE NUMERIC FEATURES =================
scaler = StandardScaler()
numeric_cols = ["price", "user_activity_count", "category_popularity"]
df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

# ================= FEATURES / TARGET =================
X = df.drop(columns=["event_type", "user_session", "category_code", "label"])
y = df["label"]

# ================= TRAIN / TEST SPLIT =================
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# ================= MODEL =================
print("🧠 Training RandomForest model...")

model = RandomForestClassifier(
    n_estimators=100,
    max_depth=10,
    class_weight="balanced",
    random_state=42,
    n_jobs=-1,
)

model.fit(X_train, y_train)

# ================= EVALUATION =================
y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]

print("\n📊 Classification Report:")
print(classification_report(y_test, y_pred))
print("ROC AUC:", roc_auc_score(y_test, y_proba))

# ================= SAVE MODEL =================
os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
joblib.dump(
    {"model": model, "scaler": scaler, "features": X.columns.tolist()}, MODEL_PATH
)
print(f"💾 Model saved to {MODEL_PATH}")
