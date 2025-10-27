from pathlib import Path
import numpy as np
import pandas as pd
import joblib
from scipy import sparse
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
from typing import List, Dict

ART = Path("artifacts"); ART.mkdir(exist_ok=True)
DATA = Path("data/games_features.csv")
MODEL = ART / "knn.joblib"
IDS = ART / "game_ids.npy"
SCALER = ART / "scaler.joblib"

class KNNRecommender:
    def __init__(self):
        self._loaded = False
        self.knn = None
        self.game_ids = None
        self.id2idx = None
        self.X = None

    # ---------- offline training ----------
    def train_from_csv(self):
        if not DATA.exists():
            raise RuntimeError(f"Features file not found: {DATA}")
        df = pd.read_csv(DATA)
        game_ids = df["game_id"].astype(int).to_numpy()
        X = df.drop(columns=["game_id"]).astype("float32")

        num_cols = [c for c in ["price_norm","rating_avg","nb_reviews"] if c in X.columns]
        scaler = None
        if num_cols:
            scaler = StandardScaler(with_mean=False)
            X[num_cols] = scaler.fit_transform(X[num_cols]).astype("float32")

        X_sparse = sparse.csr_matrix(X.values, dtype="float32")

        knn = NearestNeighbors(metric="cosine", algorithm="brute", n_neighbors=50, n_jobs=-1)
        knn.fit(X_sparse)

        joblib.dump(knn, MODEL)
        np.save(IDS, game_ids)
        if scaler: joblib.dump(scaler, SCALER)
        print(f"[train] saved artifacts -> {MODEL}, {IDS}, {SCALER if scaler else '(no scaler)'}")

    # ---------- runtime ----------
    def _ensure_loaded(self):
        if self._loaded: return
        if not (MODEL.exists() and IDS.exists()):
            raise RuntimeError("KNN artifacts not found. Train the model first.")
        self.knn = joblib.load(MODEL)
        self.game_ids = np.load(IDS)
        self.id2idx = {int(g): i for i, g in enumerate(self.game_ids)}
        self.X = self.knn._fit_X
        self._loaded = True

    def recommend_for_items(self, purchased: List[int], k_per_item: int = 50, top: int = 50) -> List[Dict]:
        self._ensure_loaded()
        seen = set(int(g) for g in purchased)
        scores: Dict[int, float] = {}

        for gid in seen:
            idx = self.id2idx.get(gid)
            if idx is None:
                continue
            distances, indices = self.knn.kneighbors(self.X[idx:idx+1], n_neighbors=k_per_item, return_distance=True)
            sims = 1.0 - distances[0]
            neigh = indices[0]
            for j, sim in zip(neigh, sims):
                rec_gid = int(self.game_ids[j])
                if rec_gid in seen or rec_gid == gid:
                    continue
                scores[rec_gid] = scores.get(rec_gid, 0.0) + float(sim)

        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:top]
        return [ {"gameId": g, "score": s} for g, s in ranked ]

# singleton importable par main.py
knn = KNNRecommender()

if __name__ == "__main__":
    # Entrainer depuis le CSV
    knn.train_from_csv()
