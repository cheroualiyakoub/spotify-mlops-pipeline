import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin

class ArtistPopularityEncoder(BaseEstimator, TransformerMixin):
    """
    Encodes artist popularity based on the mean popularity of their songs in the training set.
    - Uses the target `y` during `fit`, so it works with train/test splits.
    - Handles unseen artists in the test set using the global training mean.
    
    Parameters:
    -----------
    artist_col : str
        Name of the column containing artist names (must exist in `X`).
    handle_unseen : str (default='global_mean')
        Strategy for unseen artists: 'global_mean' or 'default_value'.
    default_value : float (optional)
        Custom default value if handle_unseen='default_value'.
    """
    
    def __init__(self, artist_col='artist_name', handle_unseen='global_mean', default_value=None):
        self.artist_col = artist_col
        self.handle_unseen = handle_unseen
        self.default_value = default_value
        self.artist_popularity_map_ = None  # Artist -> mean popularity
        self.global_mean_ = None  # Fallback for unseen artists
        
    def fit(self, X, y=None):
        if y is None:
            raise ValueError("This encoder requires the target `y` during fitting.")
            
        # Group target `y` by artist in `X` to compute mean popularity
        self.artist_popularity_map_ = (
            pd.Series(y)
            .groupby(X[self.artist_col])
            .mean()
            .to_dict()
        )
        
        # Global mean of the target for fallback
        self.global_mean_ = y.mean()
        
        return self
    
    def transform(self, X):
        X = X.copy()
        
        # Map artist popularity from precomputed training data
        X['encoded_artist_popularity'] = X[self.artist_col].map(self.artist_popularity_map_)
        
        # Handle unseen artists
        if self.handle_unseen == 'global_mean':
            fill_value = self.global_mean_
        elif self.handle_unseen == 'default_value':
            fill_value = self.default_value
        else:
            raise ValueError(f"Unknown handle_unseen: {self.handle_unseen}")
            
        X['encoded_artist_popularity'] = X['encoded_artist_popularity'].fillna(fill_value)
        
        return X