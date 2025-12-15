import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import psycopg2
import redis

# ================= CONFIG =================
POSTGRES_CONFIG = {
    "host": "postgres",
    "database": "ecommerce",
    "user": "user",
    "password": "password",
}

REDIS_HOST = "redis"
REDIS_PORT = 6379

# ================= INIT =================
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
conn = psycopg2.connect(**POSTGRES_CONFIG)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "E-commerce Dashboard"

# ================= DASHBOARD LAYOUT =================
app.layout = dbc.Container(
    [
        html.H1("E-commerce Real-time Dashboard"),
        html.Hr(),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="category-popularity"), width=6),
                dbc.Col(dcc.Graph(id="user-activity"), width=6),
            ]
        ),
        dcc.Interval(
            id="interval-component", interval=10 * 1000, n_intervals=0  # 10 seconds
        ),
    ],
    fluid=True,
)


# ================= CALLBACKS =================
@app.callback(
    Output("category-popularity", "figure"),
    Output("user-activity", "figure"),
    Input("interval-component", "n_intervals"),
)
def update_graphs(n):
    # --- Category Popularity from Redis ---
    categories = []
    counts = []
    for key in r.scan_iter("category_stats:*"):
        categories.append(key.decode().split(":")[1])
        counts.append(int(r.get(key)))
    df_cat = pd.DataFrame({"category": categories, "count": counts})
    fig_cat = px.bar(df_cat, x="category", y="count", title="Category Popularity")

    # --- User Activity from Redis ---
    sessions = []
    activity_counts = []
    for key in r.scan_iter("user_activity:*"):
        sessions.append(key.decode().split(":")[1])
        activity_counts.append(int(r.get(key)))
    df_user = pd.DataFrame(
        {"user_session": sessions, "activity_count": activity_counts}
    )
    fig_user = px.bar(
        df_user, x="user_session", y="activity_count", title="User Activity"
    )

    return fig_cat, fig_user


# ================= RUN DASHBOARD =================
if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True)
