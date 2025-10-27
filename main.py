from fastapi import FastAPI, HTTPException
from models import RecommendationRequest, RecommendationResponse, RecommendationItem
from recommendation import knn

app = FastAPI(title="GamesUP Reco API", version="1.0")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/api/recommendations", response_model=RecommendationResponse)
def recommend(payload: RecommendationRequest):
    if not payload.purchasedGames:
        # si aucun historique: renvoie vide
        return RecommendationResponse(recommendations=[])
    try:
        recs = knn.recommend_for_items(payload.purchasedGames)
        return RecommendationResponse(recommendations=[RecommendationItem(**r) for r in recs])
    except RuntimeError as e:
        # artefacts manquants / non entrainé
        raise HTTPException(status_code=503, detail=str(e))
