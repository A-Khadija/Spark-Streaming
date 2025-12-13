import pandas as pd
import numpy as np
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from datetime import datetime

# --- CONFIGURATION ---
DATA_PATH = "../data/2019-Nov.csv"
MODEL_PATH = "../serving/model.pkl"
ENCODER_PATH = "../serving/encoders.pkl"

def load_and_prep_data(filepath, limit=5000000):
    print(f"Loading data from {filepath} (Limit: {limit} rows)...")
    df = pd.read_csv(filepath, nrows=limit, 
                     usecols=['event_time', 'event_type', 'product_id', 
                              'category_code', 'brand', 'price', 
                              'user_id', 'user_session'])
    
    print("Engineering features...")
    df['category_code'] = df['category_code'].fillna("unknown.unknown")
    df['category_level1'] = df['category_code'].apply(lambda x: x.split('.')[0])
    df['category_level2'] = df['category_code'].apply(lambda x: x.split('.')[1] if '.' in x else "unknown")
    
    # --- IMPROVEMENT 1: Smarter Price Ratio (Level 2) ---
    # Compare price to the specific sub-category (e.g. smartphone vs smartphones), not the broad category
    category_avg_price = df.groupby('category_level2')['price'].transform('mean')
    df['price_ratio'] = df['price'] / category_avg_price
    df['price_ratio'] = df['price_ratio'].fillna(1.0)
    
    # --- IMPROVEMENT 2: Add Hour of Day ---
    df['event_time'] = pd.to_datetime(df['event_time'])
    df['event_weekday'] = df['event_time'].dt.weekday
    df['event_hour'] = df['event_time'].dt.hour  # New Feature
    
    df['activity_count'] = df.groupby('user_session').cumcount() + 1
    
    return df

def create_target_variable(df):
    print("Creating target variable (is_purchased)...")
    purchased_sessions = set(df[df['event_type'] == 'purchase']['user_session'].unique())
    cart_events = df[df['event_type'] == 'cart'].copy()
    cart_events['is_purchased'] = cart_events['user_session'].apply(lambda x: 1 if x in purchased_sessions else 0)
    
    print(f"   Found {len(cart_events)} cart events.")
    print(f"   Purchase Rate: {cart_events['is_purchased'].mean():.2%}")
    return cart_events

def train():
    df = load_and_prep_data(DATA_PATH, limit=5000000) 
    train_df = create_target_variable(df)
    
    if len(train_df) < 10:
        print("Error: Not enough 'cart' events.")
        return

    le_cat1 = LabelEncoder()
    le_cat2 = LabelEncoder()
    le_brand = LabelEncoder()
    
    train_df['brand'] = train_df['brand'].fillna(train_df['category_code']).astype(str)
    
    train_df['category_level1_enc'] = le_cat1.fit_transform(train_df['category_level1'])
    train_df['category_level2_enc'] = le_cat2.fit_transform(train_df['category_level2'])
    train_df['brand_enc'] = le_brand.fit_transform(train_df['brand'])
    
    # Added 'event_hour' to features
    features = ['category_level1_enc', 'category_level2_enc', 'brand_enc', 
                'price', 'price_ratio', 'event_weekday', 'event_hour', 'activity_count']
    X = train_df[features]
    y = train_df['is_purchased']
    
    print(f"Training Random Forest on {len(X)} samples...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # --- IMPROVEMENT 3: Deeper Model ---
    # Increased max_depth from 12 to 25 to allow learning complex patterns
    # Increased estimators to 200 for stability
    # n_jobs=-1 uses all CPU cores to make it faster
    clf = RandomForestClassifier(n_estimators=200, max_depth=25, n_jobs=-1, random_state=42)
    clf.fit(X_train, y_train)
    
    score = clf.score(X_test, y_test)
    print(f"Model Trained! Accuracy: {score:.4f}")
    
    artifacts = {
        'model': clf,
        'le_cat1': le_cat1,
        'le_cat2': le_cat2,
        'le_brand': le_brand
    }
    joblib.dump(artifacts, MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")

if __name__ == "__main__":
    train()