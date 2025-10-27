from pydantic import BaseModel, Field
from typing import List

class RecommendationItem(BaseModel):
    gameId: int
    score: float

class RecommendationRequest(BaseModel):
    userId: int
    purchasedGames: List[int] = Field(default_factory=list)
    likedCategories: List[int] = Field(default_factory=list)
    likedAuthors: List[int] = Field(default_factory=list)

class RecommendationResponse(BaseModel):
    recommendations: List[RecommendationItem]
