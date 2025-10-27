import os
import random
import string
from datetime import datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ---- Config lecture .env ----
load_dotenv()

MYSQL_URL = os.getenv("MYSQL_URL")
if not MYSQL_URL:
    raise RuntimeError("Missing MYSQL_URL in .env")

T_G  = os.getenv("TABLE_GAMES", "game")
T_A  = os.getenv("TABLE_AUTHORS", "author")
T_C  = os.getenv("TABLE_CATEGORIES", "category")
T_P  = os.getenv("TABLE_PUBLISHERS", "publisher")
J_GA = os.getenv("JUNCTION_GAME_AUTHOR", "game_author")
J_GC = os.getenv("JUNCTION_GAME_CATEGORY", "game_category")
T_R  = os.getenv("TABLE_REVIEWS", "review")
T_U  = os.getenv("TABLE_USERS", "users")

# ---- Paramétrage volume (modifiable) ----
N_PUBLISHERS  = int(os.getenv("SEED_N_PUBLISHERS", 40))
N_AUTHORS     = int(os.getenv("SEED_N_AUTHORS", 120))
N_CATEGORIES  = int(os.getenv("SEED_N_CATEGORIES", 60))
N_GAMES       = int(os.getenv("SEED_N_GAMES", 400))
MIN_CATS_PER_GAME = 1
MAX_CATS_PER_GAME = 3
MIN_AUTH_PER_GAME = 1
MAX_AUTH_PER_GAME = 2

# Volume d’avis par user (aléatoire dans cette fourchette)
MIN_REVIEWS_PER_USER = int(os.getenv("SEED_MIN_REVIEWS_PER_USER", 20))
MAX_REVIEWS_PER_USER = int(os.getenv("SEED_MAX_REVIEWS_PER_USER", 50))

rnd = random.Random(42)  # reproductible

# ---- Helpers ----
def rand_name(prefix, i=None):
    suf = i if i is not None else rnd.randint(1000, 9999)
    return f"{prefix} {suf}"

def rand_title():
    # petits morceaux pour des noms de jeux plausibles
    adj = ["Epic", "Mystic", "Quantum", "Turbo", "Hidden", "Ancient", "Cosmic", "Royal", "Secret", "Silent"]
    nouns = ["Frontier", "Dynasty", "Odyssey", "Empire", "Quest", "Labyrinth", "Garden", "Alliance", "Echoes", "Uprising"]
    return f"{rnd.choice(adj)} {rnd.choice(nouns)} {rnd.randint(1, 999)}"

def slug_suffix(n=4):
    return "".join(rnd.choice(string.ascii_uppercase + string.digits) for _ in range(n))

def main():
    engine = create_engine(MYSQL_URL, pool_pre_ping=True)
    with engine.begin() as cx:  # transaction auto-commit
        # 1) Seed Publishers, Authors, Categories (INSERT IGNORE pour éviter les doublons)
        # -------------------------------------------------------------------------
        print("Seeding publishers/authors/categories (INSERT IGNORE)...")

        # publishers
        pubs = [rand_name("Publisher", i) for i in range(1, N_PUBLISHERS + 1)]
        cx.execute(text(f"INSERT IGNORE INTO `{T_P}` (name) VALUES " + ",".join(["(:n"+str(i)+")" for i in range(len(pubs))])),
                   {f"n{i}": pubs[i] for i in range(len(pubs))})

        # authors
        auths = [rand_name("Author", i) for i in range(1, N_AUTHORS + 1)]
        cx.execute(text(f"INSERT IGNORE INTO `{T_A}` (name) VALUES " + ",".join(["(:n"+str(i)+")" for i in range(len(auths))])),
                   {f"n{i}": auths[i] for i in range(len(auths))})

        # categories
        cats = [rand_name("Category", i) for i in range(1, N_CATEGORIES + 1)]
        cx.execute(text(f"INSERT IGNORE INTO `{T_C}` (name) VALUES " + ",".join(["(:n"+str(i)+")" for i in range(len(cats))])),
                   {f"n{i}": cats[i] for i in range(len(cats))})

        # 2) Récupérer les IDs
        # --------------------
        pub_ids = [row[0] for row in cx.execute(text(f"SELECT id FROM `{T_P}`")).fetchall()]
        auth_ids = [row[0] for row in cx.execute(text(f"SELECT id FROM `{T_A}`")).fetchall()]
        cat_ids  = [row[0] for row in cx.execute(text(f"SELECT id FROM `{T_C}`")).fetchall()]

        # 3) Créer des games
        # -------------------
        print("Seeding games + liaisons (authors/categories)...")
        # noms aléatoires mais uniques (grâce au suffix)
        new_games = []
        for i in range(N_GAMES):
            name = f"{rand_title()}-{slug_suffix(5)}"
            price = round(rnd.uniform(9.9, 79.9), 2)
            stock = rnd.randint(0, 200)
            pub_id = rnd.choice(pub_ids) if pub_ids else None
            desc = f"Generated game #{i+1} for recommendation seeding."
            new_games.append((name, desc, price, stock, pub_id))

        # INSERT IGNORE sur name (unique dans ton schéma)
        if new_games:
            values_sql = ",".join([f"(:n{i}, :d{i}, :p{i}, :s{i}, :pub{i})" for i in range(len(new_games))])
            params = {}
            for i, (name, desc, price, stock, pub_id) in enumerate(new_games):
                params[f"n{i}"] = name
                params[f"d{i}"] = desc
                params[f"p{i}"] = price
                params[f"s{i}"] = stock
                params[f"pub{i}"] = pub_id
            cx.execute(text(
                f"INSERT IGNORE INTO `{T_G}` (name, description, price, stock, publisher_id) VALUES {values_sql}"
            ), params)

        # Récupérer les IDs des jeux (incluant les existants)
        game_rows = cx.execute(text(f"SELECT id FROM `{T_G}`")).fetchall()
        game_ids = [r[0] for r in game_rows]

        # 4) Lier games -> authors / categories (INSERT IGNORE)
        # -----------------------------------------------------
        # On génère quelques liaisons par jeu
        ga_pairs = set()
        gc_pairs = set()
        for gid in game_ids:
            for _ in range(rnd.randint(MIN_AUTH_PER_GAME, MAX_AUTH_PER_GAME)):
                if auth_ids:
                    ga_pairs.add((gid, rnd.choice(auth_ids)))
            for _ in range(rnd.randint(MIN_CATS_PER_GAME, MAX_CATS_PER_GAME)):
                if cat_ids:
                    gc_pairs.add((gid, rnd.choice(cat_ids)))

        if ga_pairs:
            vals = ",".join([f"(:g{i}, :a{i})" for i in range(len(ga_pairs))])
            params = {}
            for i, (g, a) in enumerate(ga_pairs):
                params[f"g{i}"] = g
                params[f"a{i}"] = a
            cx.execute(text(f"INSERT IGNORE INTO `{J_GA}` (game_id, author_id) VALUES {vals}"), params)

        if gc_pairs:
            vals = ",".join([f"(:g{i}, :c{i})" for i in range(len(gc_pairs))])
            params = {}
            for i, (g, c) in enumerate(gc_pairs):
                params[f"g{i}"] = g
                params[f"c{i}"] = c
            cx.execute(text(f"INSERT IGNORE INTO `{J_GC}` (game_id, category_id) VALUES {vals}"), params)

        # 5) Créer des reviews pour les utilisateurs existants
        # ----------------------------------------------------
        # On ne crée pas de nouveaux users (pour ne pas casser la sécurité) : on réutilise ceux déjà en DB.
        user_ids = [row[0] for row in cx.execute(text(f"SELECT id FROM `{T_U}`")).fetchall()]
        if not user_ids:
            print("⚠️ Aucun user dans la table `users` → impossible de générer des reviews.")
            print("➡️ Crée d’abord quelques utilisateurs via l’API Spring, puis relance ce script.")
            return

        print("Seeding reviews (INSERT IGNORE, unique (user_id, game_id))...")
        now = datetime.utcnow()
        review_inserts = []
        for uid in user_ids:
            nb = rnd.randint(MIN_REVIEWS_PER_USER, MAX_REVIEWS_PER_USER)
            sampled_games = rnd.sample(game_ids, k=min(nb, len(game_ids)))
            for gid in sampled_games:
                rating = rnd.randint(1,5)
                delta_days = rnd.randint(0, 365)
                created = now - timedelta(days=delta_days)
                updated = created
                comment = f"Auto-review by user {uid} for game {gid} (seed)"
                review_inserts.append((comment, created, rating, updated, gid, uid))

        # batch insert IGNORE (respecte la contrainte unique (user_id, game_id))
        if review_inserts:
            vals = ",".join([f"(:c{i}, :cr{i}, :r{i}, :up{i}, :g{i}, :u{i})" for i in range(len(review_inserts))])
            params = {}
            for i, (comment, created, rating, updated, gid, uid) in enumerate(review_inserts):
                params[f"c{i}"] = comment
                params[f"cr{i}"] = created
                params[f"r{i}"] = rating
                params[f"up{i}"] = updated
                params[f"g{i}"] = gid
                params[f"u{i}"] = uid
            cx.execute(text(
                f"INSERT IGNORE INTO `{T_R}` (comment, created_at, rating, updated_at, game_id, user_id) "
                f"VALUES {vals}"
            ), params)

        print("✅ Seed terminé.")

if __name__ == "__main__":
    main()
