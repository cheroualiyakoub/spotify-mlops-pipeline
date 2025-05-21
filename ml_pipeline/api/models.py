from pydantic import BaseModel
from typing import List, Optional

class SongFeatures(BaseModel):
    acousticness: float
    danceability: float
    energy: float
    instrumentalness: float
    key: int
    liveness: float
    loudness: float
    mode: int
    speechiness: float
    tempo: float
    time_signature: int
    valence: float
    
class PredictionResponse(BaseModel):
    popularity: float
    model_version: str