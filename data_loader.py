import os
from dotenv import load_dotenv; load_dotenv()
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path

OUT = Path("data"); OUT.mkdir(parents=True, exist_ok=True)

def env(name, default=None):
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing env {name}")
    return v

def build_features_csv():
    url = env("MYSQL_URL")
    T_G  = env("TABLE_GAMES", "game")
    T_R  = env("TABLE_REVIEWS", "review")
    J_GC = env("JUNCTION_GAME_CATEGORY", "game_category")
    J_GA = env("JUNCTION_GAME_AUTHOR", "game_author")
    top_c = int(env("TOP_CATEGORIES", "200"))
    top_a = int(env("TOP_AUTHORS", "500"))

    engine = create_engine(url)

    # jeux (id + prix)
    games = pd.read_sql(text(f"""
        SELECT g.id AS game_id, g.price
        FROM `{T_G}` g
    """), engine)

    # liaisons catégories
    gc = pd.read_sql(text(f"SELECT game_id, category_id FROM `{J_GC}`"), engine)

    # liaisons auteurs
    ga = pd.read_sql(text(f"SELECT game_id, author_id FROM `{J_GA}`"), engine)

    # agrégats avis
    rev = pd.read_sql(text(f"""
        SELECT r.game_id, AVG(r.rating) AS rating_avg, COUNT(*) AS nb_reviews
        FROM `{T_R}` r
        GROUP BY r.game_id
    """), engine)

    # limiter la dimension des one-hot
    if not gc.empty:
        keep_c = gc["category_id"].value_counts().head(top_c).index
        gc = gc[gc["category_id"].isin(keep_c)]
        cat = (gc.assign(val=1)
                 .pivot_table(index="game_id", columns="category_id", values="val", fill_value=0))
        cat.columns = [f"cat_{c}" for c in cat.columns]
    else:
        cat = pd.DataFrame(index=games["game_id"]).assign(dummy_cat=0)

    if not ga.empty:
        keep_a = ga["author_id"].value_counts().head(top_a).index
        ga = ga[ga["author_id"].isin(keep_a)]
        auth = (ga.assign(val=1)
                  .pivot_table(index="game_id", columns="author_id", values="val", fill_value=0))
        auth.columns = [f"auth_{c}" for c in auth.columns]
    else:
        auth = pd.DataFrame(index=games["game_id"]).assign(dummy_auth=0)

    # features finales
    feats = games.set_index("game_id")[["price"]].copy()
    feats["price_norm"] = feats["price"].fillna(0).astype(float)
    feats = feats.drop(columns=["price"])

    feats = feats.join(rev.set_index("game_id")[["rating_avg","nb_reviews"]], how="left")\
                 .fillna({"rating_avg":0.0, "nb_reviews":0})
    feats = feats.join(cat, how="left").fillna(0).join(auth, how="left").fillna(0)

    out = OUT / "games_features.csv"
    feats.reset_index().to_csv(out, index=False)
    print(f"[data_loader] wrote {out} shape={feats.shape}")

if __name__ == "__main__":
    build_features_csv()
